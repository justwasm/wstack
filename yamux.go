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
)

// yamuxOverWSDialer lazily establishes a yamux session over a single
// WebSocket connection and opens a lightweight stream per Dial call.
// The remote side (Cloudflare Worker) demuxes streams and connects
// each to its target address via raw TCP.
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

	// Send target address as first data on the stream.
	// The Worker reads this before forwarding any subsequent data.
	if _, err := stream.Write([]byte(addr)); err != nil {
		stream.Close()
		return nil, fmt.Errorf("send target address: %w", err)
	}

	return stream, nil
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
// The Worker demuxes streams and connects each to the target address
// via raw TCP.
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

	return &http.Transport{
		DialContext:     dialer.DialContext,
		TLSClientConfig: insecure,
		MaxIdleConns:    100,
		// Yamux streams have negligible setup cost, but we still want
		// keep-alive to reuse streams for the same host.
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	}, nil
}
