package wstack

import "crypto/tls"

// insecure is a shared TLS config that skips certificate
// verification. Used by all transports when connecting through WebSocket
// relay workers that may use self-signed or ephemeral certificates.
var insecure = &tls.Config{
	InsecureSkipVerify: true,
}
