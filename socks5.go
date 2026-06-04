package wstack

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"

	"github.com/btwiuse/wsdial"
	"golang.org/x/net/proxy"
)

// wsForwardDialer connects to the SOCKS5 server through a WebSocket relay.
type wsForwardDialer struct {
	relayURL *url.URL
}

func (d *wsForwardDialer) Dial(network, addr string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, addr)
}

func (d *wsForwardDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return wsdial.Dial(ctx, d.relayURL, nil)
}

// NewSOCKS5OverWSTransport creates an http.Transport that routes through a
// SOCKS5 proxy via a WebSocket-over-TLS relay.
//
//   - wsRelayHost: the WebSocket relay server (e.g. "websocket-tcp-proxy.dev")
//   - socks5Host:  the SOCKS5 proxy server behind the relay
//   - socks5Port:  the SOCKS5 proxy port
func NewSOCKS5OverWSTransport(wsRelayHost, socks5Host, socks5Port string) (*http.Transport, error) {
	forwardDial := &wsForwardDialer{relayURL: relayURL(wsRelayHost, socks5Host, socks5Port)}

	socks5Dialer, err := proxy.SOCKS5("tcp", socks5Host+":"+socks5Port, nil, forwardDial)
	if err != nil {
		return nil, fmt.Errorf("create SOCKS5 dialer: %w", err)
	}

	ctxDialer := socks5Dialer.(proxy.ContextDialer)

	return &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return ctxDialer.DialContext(ctx, network, addr)
		},
		TLSClientConfig: insecure,
	}, nil
}
