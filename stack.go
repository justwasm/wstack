// Package wstack implements a WASM-friendly userland network stack over
// WebSocket. It enables WebAssembly programs to establish TCP and UDP
// connections by tunneling IP packets through a WebSocket connection to a
// server-side proxy.
//
// # Architecture
//
// Client (WASM or native) side:
//
//	Application (net.Conn)
//	      ↓
//	Userland TCP/IP stack (gVisor, AllowExternalLoopbackTraffic)
//	      ↓ raw IP packets
//	WebSocket connection
//	      ↓
//	Server-side proxy (cmd/wsproxy)
//	      ↓
//	Internet
//
// # Usage
//
//	s, err := wstack.New(ctx, "ws://proxy.example.com/ws", wstack.Options{
//	    LocalAddrs: []netip.Addr{netip.MustParseAddr("10.0.0.2")},
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer s.Close()
//
//	conn, err := s.Dial(ctx, "tcp", "example.com:80")
package wstack

import (
	"context"
	"fmt"
	"net"
	"net/netip"

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
	"nhooyr.io/websocket"
)

const defaultNICID = tcpip.NICID(1)

// Options configures a [Stack].
type Options struct {
	// LocalAddrs are the IP addresses assigned to the virtual network
	// interface. If empty, defaults to [10.0.0.2].
	LocalAddrs []netip.Addr

	// MTU is the maximum transmission unit for the virtual interface.
	// If 0, defaults to 1420.
	MTU int

	// DialOptions are optional options passed to the underlying
	// [websocket.Dial] call.
	DialOptions *websocket.DialOptions
}

// Stack is a userland TCP/IP network stack tunneled over a WebSocket
// connection. It is safe for concurrent use.
type Stack struct {
	gs     *gstack.Stack
	ep     *channel.Endpoint
	cancel context.CancelFunc
	done   chan struct{}
}

// New connects to the WebSocket proxy at wsURL and returns a ready Stack.
// The Stack remains active until [Stack.Close] is called or ctx is cancelled.
func New(ctx context.Context, wsURL string, opts Options) (*Stack, error) {
	if opts.MTU == 0 {
		opts.MTU = 1420
	}
	if len(opts.LocalAddrs) == 0 {
		opts.LocalAddrs = []netip.Addr{netip.MustParseAddr("10.0.0.2")}
	}

	// Build a gVisor userspace TCP/IP stack.
	// AllowExternalLoopbackTraffic is required so that packets whose source
	// or destination address is a loopback address (e.g. 127.0.0.1) are not
	// silently dropped when they arrive on a non-loopback NIC.
	gs := gstack.New(gstack.Options{
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
		HandleLocal: true,
	})

	ep := channel.New(512, uint32(opts.MTU), "")
	if err := gs.CreateNIC(defaultNICID, ep); err != nil {
		return nil, fmt.Errorf("wstack: CreateNIC: %v", err)
	}

	// Assign each requested local address to the NIC.
	for _, addr := range opts.LocalAddrs {
		var proto tcpip.NetworkProtocolNumber
		if addr.Is4() {
			proto = ipv4.ProtocolNumber
		} else {
			proto = ipv6.ProtocolNumber
		}
		pa := tcpip.ProtocolAddress{
			Protocol:          proto,
			AddressWithPrefix: tcpip.AddrFromSlice(addr.AsSlice()).WithPrefix(),
		}
		if err := gs.AddProtocolAddress(defaultNICID, pa, gstack.AddressProperties{}); err != nil {
			ep.Close()
			gs.Close()
			return nil, fmt.Errorf("wstack: AddProtocolAddress(%v): %v", addr, err)
		}
	}

	// Default routes: send everything through this NIC.
	gs.SetRouteTable([]tcpip.Route{
		{Destination: header.IPv4EmptySubnet, NIC: defaultNICID},
		{Destination: header.IPv6EmptySubnet, NIC: defaultNICID},
	})

	// Connect to the WebSocket proxy.
	wsConn, _, err := websocket.Dial(ctx, wsURL, opts.DialOptions)
	if err != nil {
		ep.Close()
		gs.Close()
		return nil, fmt.Errorf("wstack: dial websocket %s: %w", wsURL, err)
	}
	// Allow messages large enough to hold any IP packet up to opts.MTU.
	wsConn.SetReadLimit(int64(opts.MTU+40) * 4)

	ctx, cancel := context.WithCancel(ctx)
	s := &Stack{
		gs:     gs,
		ep:     ep,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	go s.run(ctx, wsConn)
	return s, nil
}

// run manages the bidirectional bridge between the WebSocket and the gVisor
// channel endpoint. It closes the done channel when both directions stop.
func (s *Stack) run(ctx context.Context, wsConn *websocket.Conn) {
	defer close(s.done)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errc := make(chan error, 2)

	// WebSocket → gVisor: inject inbound IP packets into the stack.
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
			s.ep.InjectInbound(proto, pkt)
			pkt.DecRef()
		}
	}()

	// gVisor → WebSocket: forward outbound IP packets to the proxy.
	go func() {
		defer cancel()
		for {
			pkt := s.ep.ReadContext(ctx)
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

	<-errc
	wsConn.Close(websocket.StatusNormalClosure, "")
	<-errc
}

// addrToFullAddr converts a netip.AddrPort to a tcpip.FullAddress.
func addrToFullAddr(ap netip.AddrPort) (tcpip.FullAddress, tcpip.NetworkProtocolNumber) {
	addr := ap.Addr()
	fa := tcpip.FullAddress{
		Port: ap.Port(),
	}
	var proto tcpip.NetworkProtocolNumber
	if addr.Is4() {
		a4 := addr.As4()
		fa.Addr = tcpip.AddrFrom4(a4)
		proto = ipv4.ProtocolNumber
	} else {
		a16 := addr.As16()
		fa.Addr = tcpip.AddrFrom16(a16)
		proto = ipv6.ProtocolNumber
	}
	return fa, proto
}

// Dial creates a network connection through the stack to the given address.
// The network must be "tcp", "tcp4", "tcp6", "udp", "udp4", or "udp6".
func (s *Stack) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	switch network {
	case "tcp", "tcp4", "tcp6":
		tcpAddr, err := net.ResolveTCPAddr(network, address)
		if err != nil {
			return nil, err
		}
		ip, ok := netip.AddrFromSlice(tcpAddr.IP)
		if !ok {
			return nil, fmt.Errorf("wstack: invalid address %q", address)
		}
		fa, proto := addrToFullAddr(netip.AddrPortFrom(ip.Unmap(), uint16(tcpAddr.Port)))
		return gonet.DialContextTCP(ctx, s.gs, fa, proto)
	case "udp", "udp4", "udp6":
		udpAddr, err := net.ResolveUDPAddr(network, address)
		if err != nil {
			return nil, err
		}
		ip, ok := netip.AddrFromSlice(udpAddr.IP)
		if !ok {
			return nil, fmt.Errorf("wstack: invalid address %q", address)
		}
		fa, proto := addrToFullAddr(netip.AddrPortFrom(ip.Unmap(), uint16(udpAddr.Port)))
		return gonet.DialUDP(s.gs, nil, &fa, proto)
	default:
		return nil, fmt.Errorf("wstack: unsupported network %q", network)
	}
}

// Listen announces on the local network address through the stack.
// The network must be "tcp", "tcp4", or "tcp6".
func (s *Stack) Listen(network, address string) (net.Listener, error) {
	tcpAddr, err := net.ResolveTCPAddr(network, address)
	if err != nil {
		return nil, err
	}
	var fa tcpip.FullAddress
	var proto tcpip.NetworkProtocolNumber
	if tcpAddr.IP == nil || tcpAddr.IP.IsUnspecified() {
		fa = tcpip.FullAddress{Port: uint16(tcpAddr.Port)}
		proto = ipv4.ProtocolNumber
	} else {
		ip, ok := netip.AddrFromSlice(tcpAddr.IP)
		if !ok {
			return nil, fmt.Errorf("wstack: invalid address %q", address)
		}
		fa, proto = addrToFullAddr(netip.AddrPortFrom(ip.Unmap(), uint16(tcpAddr.Port)))
	}
	return gonet.ListenTCP(s.gs, fa, proto)
}

// GStack returns the underlying gVisor [gstack.Stack], which provides
// low-level access to the userspace TCP/IP stack.
func (s *Stack) GStack() *gstack.Stack {
	return s.gs
}

// Close shuts down the Stack and the underlying WebSocket connection.
func (s *Stack) Close() error {
	s.cancel()
	s.gs.Close()
	s.ep.Close()
	<-s.done
	return nil
}
