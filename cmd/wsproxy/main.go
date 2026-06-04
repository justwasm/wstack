// Command wsproxy is a WebSocket-to-IP proxy server for wstack clients.
//
// It accepts WebSocket connections from wstack clients, tunnels the raw IP
// packets through a gVisor userspace TCP/IP stack, and proxies the resulting
// TCP and UDP flows to real network destinations.
//
// Usage:
//
//	wsproxy [-addr :8080] [-path /ws]
//
// Flags:
//
//	-addr   TCP address to listen on (default ":8080")
//	-path   HTTP path to handle WebSocket upgrades on (default "/ws")
//
// Example:
//
//	# Start the proxy
//	wsproxy -addr :8080 -path /ws
//
//	# Connect from a wstack client
//	s, err := wstack.New(ctx, "ws://localhost:8080/ws", wstack.Options{})
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	gstack "gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
	"gvisor.dev/gvisor/pkg/waiter"
	"nhooyr.io/websocket"
)

func main() {
	addr := flag.String("addr", ":8080", "TCP address to listen on")
	path := flag.String("path", "/ws", "HTTP path for WebSocket upgrades")
	flag.Parse()

	mux := http.NewServeMux()
	mux.HandleFunc(*path, handleWS)

	srv := &http.Server{Addr: *addr, Handler: mux}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()

	log.Printf("wsproxy: listening on %s%s", *addr, *path)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("wsproxy: %v", err)
	}
}

// handleWS upgrades an HTTP connection to a WebSocket and starts the proxy.
func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Allow any origin for proxy use; restrict in production as needed.
		InsecureSkipVerify: true,
	})
	if err != nil {
		log.Printf("wsproxy: accept: %v", err)
		return
	}

	// Allow large IP packets.
	conn.SetReadLimit(64 * 1024)

	ctx := r.Context()
	log.Printf("wsproxy: new client %s", r.RemoteAddr)
	if err := runProxy(ctx, conn); err != nil {
		log.Printf("wsproxy: client %s: %v", r.RemoteAddr, err)
	}
	log.Printf("wsproxy: client disconnected %s", r.RemoteAddr)
}

// runProxy sets up a gVisor userspace network stack for one WebSocket client
// and proxies its TCP/UDP traffic to the real network.
func runProxy(ctx context.Context, wsConn *websocket.Conn) error {
	const (
		mtu   = 1420
		nicID = tcpip.NICID(1)
	)

	// Build the gVisor userspace TCP/IP stack.
	// AllowExternalLoopbackTraffic lets the proxy forward connections that
	// happen to target loopback addresses on the proxy host (e.g. 127.0.0.1).
	s := gstack.New(gstack.Options{
		NetworkProtocols: []gstack.NetworkProtocolFactory{
			ipv4.NewProtocolWithOptions(ipv4.Options{
				AllowExternalLoopbackTraffic: true,
			}),
			ipv6.NewProtocol,
		},
		TransportProtocols: []gstack.TransportProtocolFactory{
			tcp.NewProtocol,
			udp.NewProtocol,
		},
	})

	// Create a channel-based link endpoint that we drive manually.
	ep := channel.New(512, mtu, "")

	if err := s.CreateNIC(nicID, ep); err != nil {
		return fmt.Errorf("create NIC: %v", err)
	}

	// Promiscuous mode: accept packets destined for any IP address.
	if err := s.SetPromiscuousMode(nicID, true); err != nil {
		return fmt.Errorf("set promiscuous: %v", err)
	}
	// Spoofing: allow sending packets with any source address.
	if err := s.SetSpoofing(nicID, true); err != nil {
		return fmt.Errorf("set spoofing: %v", err)
	}

	// Default routes: forward all IPv4 and IPv6 traffic through the NIC.
	s.SetRouteTable([]tcpip.Route{
		{Destination: header.IPv4EmptySubnet, NIC: nicID},
		{Destination: header.IPv6EmptySubnet, NIC: nicID},
	})

	// TCP forwarder: for each incoming TCP connection, dial the real
	// destination and copy data bidirectionally.
	tcpFwd := tcp.NewForwarder(s, 0, 65535, func(req *tcp.ForwarderRequest) {
		id := req.ID()
		dst := &net.TCPAddr{
			IP:   id.LocalAddress.AsSlice(),
			Port: int(id.LocalPort),
		}

		outConn, err := net.DialTCP("tcp", nil, dst)
		if err != nil {
			req.Complete(true) // Send TCP RST to the client.
			return
		}

		var wq waiter.Queue
		inEP, tcpErr := req.CreateEndpoint(&wq)
		req.Complete(false)
		if tcpErr != nil {
			outConn.Close()
			return
		}

		inConn := gonet.NewTCPConn(&wq, inEP)
		go forwardConns(inConn, outConn)
	})
	s.SetTransportProtocolHandler(tcp.ProtocolNumber, tcpFwd.HandlePacket)

	// UDP forwarder: for each incoming UDP flow, dial the real destination
	// and relay datagrams bidirectionally.
	udpFwd := udp.NewForwarder(s, func(req *udp.ForwarderRequest) {
		id := req.ID()
		dst := &net.UDPAddr{
			IP:   id.LocalAddress.AsSlice(),
			Port: int(id.LocalPort),
		}

		var wq waiter.Queue
		inEP, udpErr := req.CreateEndpoint(&wq)
		if udpErr != nil {
			return
		}

		inConn := gonet.NewUDPConn(&wq, inEP)

		outConn, err := net.DialUDP("udp", nil, dst)
		if err != nil {
			inConn.Close()
			return
		}

		go forwardConns(inConn, outConn)
	})
	s.SetTransportProtocolHandler(udp.ProtocolNumber, udpFwd.HandlePacket)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errc := make(chan error, 2)

	// WebSocket → gVisor: inject incoming IP packets into the stack.
	go func() {
		defer cancel()
		for {
			_, data, err := wsConn.Read(ctx)
			if err != nil {
				errc <- err
				return
			}
			if len(data) == 0 {
				continue
			}

			var proto tcpip.NetworkProtocolNumber
			switch data[0] >> 4 {
			case 4:
				proto = ipv4.ProtocolNumber
			case 6:
				proto = ipv6.ProtocolNumber
			default:
				continue // Drop unknown packets.
			}

			pkt := gstack.NewPacketBuffer(gstack.PacketBufferOptions{
				Payload: buffer.MakeWithData(data),
			})
			ep.InjectInbound(proto, pkt)
			pkt.DecRef()
		}
	}()

	// gVisor → WebSocket: forward outgoing IP packets to the client.
	go func() {
		defer cancel()
		for {
			pkt := ep.ReadContext(ctx)
			if pkt == nil {
				errc <- ctx.Err()
				return
			}
			buf := pkt.ToBuffer()
			data := buf.Flatten()
			pkt.DecRef()

			if err := wsConn.Write(ctx, websocket.MessageBinary, data); err != nil {
				errc <- err
				return
			}
		}
	}()

	// Wait for the first direction to fail.
	err := <-errc
	wsConn.Close(websocket.StatusNormalClosure, "")
	s.Close()
	ep.Close()
	return err
}

// forwardConns copies data between two net.Conn values until either side
// closes or errors.
func forwardConns(a, b net.Conn) {
	defer a.Close()
	defer b.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		io.Copy(a, b) //nolint:errcheck
	}()
	io.Copy(b, a) //nolint:errcheck
	<-done
}
