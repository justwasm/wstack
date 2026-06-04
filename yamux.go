package wstack

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/btwiuse/wsdial"
	"github.com/hashicorp/yamux"
	"golang.org/x/net/proxy"
)

// yamuxOverWSDialer lazily establishes a yamux session over a single
// WebSocket connection and opens a bare stream per Dial call.
// Target address negotiation is left to the caller (e.g. SOCKS5).
type yamuxOverWSDialer struct {
	mu      sync.RWMutex
	session *yamux.Session
	wsURL   *url.URL
}

func (d *yamuxOverWSDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	session, err := d.getSession(ctx)
	if err != nil {
		return nil, err
	}

	stream, err := session.Open()
	if err != nil {
		// Session likely dead — reset and retry once
		d.reset()
		session, err = d.getSession(ctx)
		if err != nil {
			return nil, err
		}
		stream, err = session.Open()
		if err != nil {
			return nil, err
		}
	}

	return stream, nil
}

// Dial implements proxy.Dialer — opens a bare yamux stream.
func (d *yamuxOverWSDialer) Dial(network, addr string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, addr)
}

func (d *yamuxOverWSDialer) getSession(ctx context.Context) (*yamux.Session, error) {
	d.mu.RLock()
	if d.session != nil {
		defer d.mu.RUnlock()
		return d.session, nil
	}
	d.mu.RUnlock()

	return d.dialSession(ctx)
}

func (d *yamuxOverWSDialer) dialSession(ctx context.Context) (*yamux.Session, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.session != nil {
		return d.session, nil
	}

	wsConn, err := wsdial.Dial(ctx, d.wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("websocket dial to yamux relay: %w", err)
	}

	// yamux client side — multiplexes streams over the WebSocket.
	// Keepalive is disabled because wsdial already sends WebSocket-level
	// pings every 15s to keep the underlying connection alive.
	yamuxCfg := yamux.DefaultConfig()
	yamuxCfg.EnableKeepAlive = false
	session, err := yamux.Client(wsConn, yamuxCfg)
	if err != nil {
		wsConn.Close()
		return nil, fmt.Errorf("yamux client: %w", err)
	}

	d.session = session
	return session, nil
}

func (d *yamuxOverWSDialer) reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.session != nil {
		d.session.Close()
		d.session = nil
	}
}

// NewYamuxOverWSTransport creates an http.Transport that routes through
// a Cloudflare Worker via a single yamux-over-WebSocket connection.
// Each yamux stream carries a SOCKS5 handshake so the relay knows the
// target address — no custom framing needed.
//
//   - wsRelayHost: the Cloudflare Worker URL (e.g. "https://yamux-proxy.example.workers.dev")
func NewYamuxOverWSTransport(wsRelayHost string) (*http.Transport, error) {
	wsURL, err := url.Parse(wsRelayHost)
	if err != nil {
		return nil, err
	}

	dialer := &yamuxOverWSDialer{
		wsURL: wsURL,
	}

	// A dummy SOCKS5 proxy address is used here. The real address is ignored
	// because our forward dialer opens yamux streams directly to the relay.
	// Using a valid host:port prevents misleading error messages if a
	// SOCKS5 handshake fails — the error would show the real target, not
	// the relay URL.
	socks5Dialer, err := proxy.SOCKS5("tcp", "0.0.0.0:0", nil, dialer)
	if err != nil {
		return nil, fmt.Errorf("create SOCKS5 dialer: %w", err)
	}

	ctxDialer := socks5Dialer.(proxy.ContextDialer)

	return &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return ctxDialer.DialContext(ctx, network, addr)
		},
		TLSClientConfig: insecure,
		MaxIdleConns:    100,
		// Yamux streams have negligible setup cost, but we still want
		// keep-alive to reuse streams for the same host.
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	}, nil
}
