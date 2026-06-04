//go:build js

package wstack

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/webtransport/webtransport"
	"golang.org/x/net/proxy"
)

// wtSession lazily establishes a WebTransport session over the browser's
// native WebTransport API. Each stream carries a SOCKS5 handshake so the
// relay knows the target address — no custom framing needed.
type wtSession struct {
	mu       sync.RWMutex
	session  *webtransport.Session
	relayURL string
}

// Dial implements proxy.Dialer — opens a raw WT stream to the relay.
// The addr is ignored because the stream itself is the connection to the
// relay; the subsequent SOCKS5 handshake carries the real target.
func (s *wtSession) Dial(network, addr string) (net.Conn, error) {
	return s.DialContext(context.Background(), network, addr)
}

func (s *wtSession) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	session, err := s.getSession(ctx)
	if err != nil {
		return nil, err
	}

	stream, err := session.Open(ctx)
	if err != nil {
		// Session likely dead — reset and retry once
		s.reset()
		session, err = s.getSession(ctx)
		if err != nil {
			return nil, err
		}
		stream, err = session.Open(ctx)
		if err != nil {
			return nil, err
		}
	}

	return stream, nil
}

func (s *wtSession) getSession(ctx context.Context) (*webtransport.Session, error) {
	s.mu.RLock()
	if s.session != nil {
		defer s.mu.RUnlock()
		return s.session, nil
	}
	s.mu.RUnlock()

	return s.dialSession(ctx)
}

func (s *wtSession) dialSession(ctx context.Context) (*webtransport.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.session != nil {
		return s.session, nil
	}

	session, err := webtransport.Dial(ctx, s.relayURL)
	if err != nil {
		return nil, fmt.Errorf("webtransport dial: %w", err)
	}

	s.session = session
	return session, nil
}

func (s *wtSession) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.session != nil {
		s.session.Close()
		s.session = nil
	}
}

// NewWebTransportTransport creates an http.Transport that routes through
// a WebTransport relay server. Each stream carries a SOCKS5 handshake to
// convey the target address — the relay must implement SOCKS5 on each
// incoming stream.
//
// This function only compiles for js/wasm. On other platforms it is
// unavailable at compile time.
//
//   - relayURL: the WebTransport relay server URL
//     (e.g. "https://wt-relay.example.com")
func NewWebTransportTransport(relayURL string) (*http.Transport, error) {
	s := &wtSession{relayURL: relayURL}

	// A dummy SOCKS5 proxy address is used here. The forward dialer opens
	// WT streams directly to the relay and ignores this address.
	socks5Dialer, err := proxy.SOCKS5("tcp", "0.0.0.0:0", nil, s)
	if err != nil {
		return nil, fmt.Errorf("create SOCKS5 dialer: %w", err)
	}

	ctxDialer := socks5Dialer.(proxy.ContextDialer)

	return &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return ctxDialer.DialContext(ctx, network, addr)
		},
		TLSClientConfig:     insecure,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	}, nil
}
