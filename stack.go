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
//	Userland TCP/IP stack (via wireguard/tun/netstack)
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

	"golang.zx2c4.com/wireguard/tun"
	"golang.zx2c4.com/wireguard/tun/netstack"
	"nhooyr.io/websocket"
)

// Options configures a [Stack].
type Options struct {
	// LocalAddrs are the IP addresses assigned to the virtual network
	// interface. If empty, defaults to [10.0.0.2].
	LocalAddrs []netip.Addr

	// DNS are the addresses of DNS servers available through the stack.
	// If empty, no DNS resolution is performed through the stack.
	DNS []netip.Addr

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
	dev    tun.Device
	tnet   *netstack.Net
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

	// Create a virtual TUN device backed by a userspace TCP/IP stack.
	dev, tnet, err := netstack.CreateNetTUN(opts.LocalAddrs, opts.DNS, opts.MTU)
	if err != nil {
		return nil, fmt.Errorf("wstack: create netstack: %w", err)
	}

	// Connect to the WebSocket proxy.
	wsConn, _, err := websocket.Dial(ctx, wsURL, opts.DialOptions)
	if err != nil {
		dev.Close()
		return nil, fmt.Errorf("wstack: dial websocket %s: %w", wsURL, err)
	}
	// Allow messages up to a generous size to accommodate large MTUs.
	wsConn.SetReadLimit(int64(opts.MTU+40) * 4)

	ctx, cancel := context.WithCancel(ctx)
	s := &Stack{
		dev:    dev,
		tnet:   tnet,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	go s.run(ctx, wsConn)
	return s, nil
}

// run manages the bidirectional bridge between the WebSocket and the TUN device.
// It closes the done channel when both directions have stopped.
func (s *Stack) run(ctx context.Context, wsConn *websocket.Conn) {
	defer close(s.done)

	errc := make(chan error, 2)
	go func() { errc <- s.wsToTun(ctx, wsConn) }()
	go func() { errc <- s.tunToWs(ctx, wsConn) }()

	// Wait for either direction to fail, then cancel both.
	<-errc
	s.cancel()
	wsConn.Close(websocket.StatusNormalClosure, "")
	<-errc
}

// wsToTun reads raw IP packets from the WebSocket and writes them to the TUN
// device so that the in-process TCP/IP stack can process them.
func (s *Stack) wsToTun(ctx context.Context, wsConn *websocket.Conn) error {
	for {
		_, data, err := wsConn.Read(ctx)
		if err != nil {
			return err
		}
		if len(data) == 0 {
			continue
		}
		if _, err := s.dev.Write([][]byte{data}, 0); err != nil {
			return err
		}
	}
}

// tunToWs reads outgoing IP packets produced by the in-process TCP/IP stack
// and forwards them to the server over the WebSocket connection.
func (s *Stack) tunToWs(ctx context.Context, wsConn *websocket.Conn) error {
	mtu, err := s.dev.MTU()
	if err != nil {
		return fmt.Errorf("wstack: get MTU: %w", err)
	}

	bufs := [][]byte{make([]byte, mtu)}
	sizes := []int{0}

	for {
		n, err := s.dev.Read(bufs, sizes, 0)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := wsConn.Write(ctx, websocket.MessageBinary, bufs[0][:sizes[i]]); err != nil {
				return err
			}
		}
	}
}

// Dial creates a network connection through the stack to the given address.
// The network must be "tcp", "tcp4", "tcp6", "udp", "udp4", or "udp6".
func (s *Stack) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	return s.tnet.DialContext(ctx, network, address)
}

// Listen announces on the local network address through the stack.
// The network must be "tcp", "tcp4", or "tcp6".
func (s *Stack) Listen(network, address string) (net.Listener, error) {
	addr, err := net.ResolveTCPAddr(network, address)
	if err != nil {
		return nil, err
	}
	return s.tnet.ListenTCP(addr)
}

// Net returns the underlying [netstack.Net], which provides additional
// methods for creating TCP, UDP, and ICMP connections.
func (s *Stack) Net() *netstack.Net {
	return s.tnet
}

// Close shuts down the Stack and the underlying WebSocket connection.
func (s *Stack) Close() error {
	s.cancel()
	err := s.dev.Close()
	<-s.done
	return err
}
