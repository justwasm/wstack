package wstack_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

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

	"github.com/justwasm/wstack"
)

// newTestProxy starts an in-process WebSocket proxy using gVisor and an
// echo TCP server, returning the WebSocket server URL, the echo server
// address, and a cleanup function.
func newTestProxy(t *testing.T) (wsURL string, echoAddr string, cleanup func()) {
	t.Helper()

	// Start a simple echo TCP server.
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	go func() {
		for {
			conn, err := echo.Accept()
			if err != nil {
				return
			}
			go io.Copy(conn, conn) //nolint:errcheck
		}
	}()

	// Start a WebSocket server that acts as the proxy.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wsConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			t.Logf("proxy accept: %v", err)
			return
		}
		wsConn.SetReadLimit(64 * 1024)

		ctx := r.Context()
		if err := runTestProxy(ctx, wsConn); err != nil {
			t.Logf("proxy run: %v", err)
		}
	}))

	return "ws" + srv.URL[4:] + "/ws", echo.Addr().String(), func() {
		srv.Close()
		echo.Close()
	}
}

// runTestProxy mirrors the logic in cmd/wsproxy/main.go.
func runTestProxy(ctx context.Context, wsConn *websocket.Conn) error {
	const (
		mtu   = 1420
		nicID = tcpip.NICID(1)
	)

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

	ep := channel.New(512, mtu, "")

	if err := s.CreateNIC(nicID, ep); err != nil {
		return fmt.Errorf("%v", err)
	}
	s.SetPromiscuousMode(nicID, true)  //nolint:errcheck
	s.SetSpoofing(nicID, true)         //nolint:errcheck

	s.SetRouteTable([]tcpip.Route{
		{Destination: header.IPv4EmptySubnet, NIC: nicID},
		{Destination: header.IPv6EmptySubnet, NIC: nicID},
	})

	tcpFwd := tcp.NewForwarder(s, 0, 65535, func(req *tcp.ForwarderRequest) {
		id := req.ID()
		dst := &net.TCPAddr{
			IP:   id.LocalAddress.AsSlice(),
			Port: int(id.LocalPort),
		}
		outConn, err := net.DialTCP("tcp", nil, dst)
		if err != nil {
			req.Complete(true)
			return
		}
		var wq waiter.Queue
		inEP, tErr := req.CreateEndpoint(&wq)
		req.Complete(false)
		if tErr != nil {
			outConn.Close()
			return
		}
		inConn := gonet.NewTCPConn(&wq, inEP)
		go func() {
			defer inConn.Close()
			defer outConn.Close()
			done := make(chan struct{})
			go func() { defer close(done); io.Copy(outConn, inConn) }() //nolint:errcheck
			io.Copy(inConn, outConn)                                     //nolint:errcheck
			<-done
		}()
	})
	s.SetTransportProtocolHandler(tcp.ProtocolNumber, tcpFwd.HandlePacket)

	udpFwd := udp.NewForwarder(s, func(req *udp.ForwarderRequest) {
		id := req.ID()
		dst := &net.UDPAddr{
			IP:   id.LocalAddress.AsSlice(),
			Port: int(id.LocalPort),
		}
		var wq waiter.Queue
		inEP, tErr := req.CreateEndpoint(&wq)
		if tErr != nil {
			return
		}
		inConn := gonet.NewUDPConn(&wq, inEP)
		outConn, err := net.DialUDP("udp", nil, dst)
		if err != nil {
			inConn.Close()
			return
		}
		go func() {
			defer inConn.Close()
			defer outConn.Close()
			done := make(chan struct{})
			go func() { defer close(done); io.Copy(outConn, inConn) }() //nolint:errcheck
			io.Copy(inConn, outConn)                                     //nolint:errcheck
			<-done
		}()
	})
	s.SetTransportProtocolHandler(udp.ProtocolNumber, udpFwd.HandlePacket)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errc := make(chan error, 2)

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
				continue
			}
			pkt := gstack.NewPacketBuffer(gstack.PacketBufferOptions{
				Payload: buffer.MakeWithData(data),
			})
			ep.InjectInbound(proto, pkt)
			pkt.DecRef()
		}
	}()

	go func() {
		defer cancel()
		for {
			pkt := ep.ReadContext(ctx)
			if pkt == nil {
				errc <- ctx.Err()
				return
			}
			b := pkt.ToBuffer()
			data := b.Flatten()
			pkt.DecRef()
			if err := wsConn.Write(ctx, websocket.MessageBinary, data); err != nil {
				errc <- err
				return
			}
		}
	}()

	err := <-errc
	wsConn.Close(websocket.StatusNormalClosure, "")
	s.Close()
	ep.Close()
	return err
}

// TestDialTCP verifies that a wstack client can dial a TCP connection through
// the WebSocket proxy and exchange data with a real TCP server.
func TestDialTCP(t *testing.T) {
	wsURL, echoAddr, cleanup := newTestProxy(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s, err := wstack.New(ctx, wsURL, wstack.Options{
		LocalAddrs: []netip.Addr{netip.MustParseAddr("10.0.0.2")},
	})
	if err != nil {
		t.Fatalf("wstack.New: %v", err)
	}
	defer s.Close()

	conn, err := s.Dial(ctx, "tcp", echoAddr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	want := "hello wstack"
	if _, err := io.WriteString(conn, want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got := make([]byte, len(want))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("Read: %v", err)
	}

	if string(got) != want {
		t.Errorf("echo: got %q, want %q", got, want)
	}
}

// TestDefaultOptions verifies that wstack.New uses sensible defaults.
func TestDefaultOptions(t *testing.T) {
	wsURL, echoAddr, cleanup := newTestProxy(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use zero Options to verify that defaults are applied.
	s, err := wstack.New(ctx, wsURL, wstack.Options{})
	if err != nil {
		t.Fatalf("wstack.New with defaults: %v", err)
	}
	defer s.Close()

	conn, err := s.Dial(ctx, "tcp", echoAddr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	msg := "defaults work"
	if _, err := io.WriteString(conn, msg); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got) != msg {
		t.Errorf("echo: got %q, want %q", got, msg)
	}
}

// TestCloseStack verifies that calling Close on a Stack shuts it down cleanly.
func TestCloseStack(t *testing.T) {
	wsURL, _, cleanup := newTestProxy(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := wstack.New(ctx, wsURL, wstack.Options{})
	if err != nil {
		t.Fatalf("wstack.New: %v", err)
	}

	// Close should not block or panic.
	if err := s.Close(); err != nil {
		t.Logf("Close: %v (non-fatal)", err) // tun close may return error on shutdown
	}
}

// TestListen verifies that a wstack client can listen for incoming TCP
// connections from within the same process (via the proxy echo path).
func TestListen(t *testing.T) {
	wsURL, _, cleanup := newTestProxy(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const localIP = "10.0.0.2"
	s, err := wstack.New(ctx, wsURL, wstack.Options{
		LocalAddrs: []netip.Addr{netip.MustParseAddr(localIP)},
	})
	if err != nil {
		t.Fatalf("wstack.New: %v", err)
	}
	defer s.Close()

	ln, err := s.Listen("tcp", ":9090")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	// Dial the listener from within the same stack.
	connCh := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		connCh <- conn
	}()

	client, err := s.Dial(ctx, "tcp", localIP+":9090")
	if err != nil {
		t.Fatalf("Dial loopback: %v", err)
	}
	defer client.Close()

	server := <-connCh
	defer server.Close()

	msg := "loopback"
	if _, err := io.WriteString(client, msg); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got) != msg {
		t.Errorf("loopback: got %q, want %q", got, msg)
	}
}
