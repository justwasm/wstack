// Package wstack provides http.RoundTripper implementations that tunnel
// HTTP traffic through various proxy backends.
//
// Available transports:
//
// CORS Proxy — URL rewriting through a CORS proxy. Works everywhere.
// See NewCorsProxyTransport.
//
// SOCKS5 over WebSocket — Routes through a SOCKS5 proxy behind a WebSocket
// relay. Works on all native Go platforms (linux/darwin). See NewSOCKS5OverWSTransport.
//
// SSH over WebSocket — Routes through an SSH server behind a WebSocket relay.
// SSH direct-tcpip channels handle the proxying. Works on all native Go
// platforms. See NewSSHOverWSTransport.
//
// Yamux over WebSocket — Multiplexes streams over a single WebSocket via yamux.
// Each stream carries a SOCKS5 handshake to convey the target address.
// All HTTP requests share one long-lived connection. Works on all native Go
// platforms. See NewYamuxOverWSTransport.
//
// WebTransport — Routes through a WebTransport (QUIC) relay from the browser.
// Each stream runs a SOCKS5 handshake to convey the target address.
// Only compiles for GOOS=js GOARCH=wasm. See NewWebTransportTransport.
package wstack
