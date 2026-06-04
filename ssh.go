package wstack

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/btwiuse/wsdial"
	"golang.org/x/crypto/ssh"
)

// sshOverWSDialer lazily establishes an SSH connection through a WebSocket
// relay and uses SSH direct-tcpip channels for proxying.
type sshOverWSDialer struct {
	mu        sync.RWMutex
	sshClient *ssh.Client
	config    sshOverWSConfig
}

type sshOverWSConfig struct {
	wsRelayHost string
	sshHost     string
	sshPort     string
	sshUser     string
	sshPass     string
}

func (d *sshOverWSDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	// First attempt with cached connection (or freshly established)
	conn, err := d.dial(ctx, network, addr)
	if err == nil {
		return conn, nil
	}

	// Connection likely dead — reset and retry once
	d.reset()
	return d.dial(ctx, network, addr)
}

func (d *sshOverWSDialer) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	// Fast path: read-lock and use cached SSH client.
	// Holding the read lock during Dial prevents reset() (which needs write lock)
	// from closing the client concurrently — eliminating the check-use gap.
	d.mu.RLock()
	if d.sshClient != nil {
		defer d.mu.RUnlock()
		return d.sshClient.Dial(network, addr)
	}
	d.mu.RUnlock()

	// Slow path: establish a new SSH connection
	return d.dialNew(ctx, network, addr)
}

func (d *sshOverWSDialer) dialNew(ctx context.Context, network, addr string) (net.Conn, error) {
	d.mu.Lock()
	// Double-check under write lock — another goroutine may have connected
	if d.sshClient != nil {
		defer d.mu.Unlock()
		return d.sshClient.Dial(network, addr)
	}
	d.mu.Unlock()

	// Connect to SSH server through the WebSocket relay
	relayURL := relayURL(d.config.wsRelayHost, d.config.sshHost, d.config.sshPort)

	wsConn, err := wsdial.Dial(ctx, relayURL, nil)
	if err != nil {
		return nil, fmt.Errorf("websocket dial to SSH server: %w", err)
	}

	sshConfig := &ssh.ClientConfig{
		User: d.config.sshUser,
		Auth: []ssh.AuthMethod{
			ssh.Password(d.config.sshPass),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	sshConn, chans, reqs, err := ssh.NewClientConn(wsConn, d.config.sshHost+":"+d.config.sshPort, sshConfig)
	if err != nil {
		wsConn.Close()
		return nil, fmt.Errorf("SSH handshake: %w", err)
	}

	client := ssh.NewClient(sshConn, chans, reqs)

	d.mu.Lock()
	if d.sshClient != nil {
		// Another goroutine already stored a client — discard ours
		client.Close()
		conn, err := d.sshClient.Dial(network, addr)
		d.mu.Unlock()
		return conn, err
	}
	d.sshClient = client
	// Dial under lock: prevents reset() from closing the brand-new client
	// before we've had a chance to use it.
	conn, err := client.Dial(network, addr)
	d.mu.Unlock()
	return conn, err
}

func (d *sshOverWSDialer) reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sshClient != nil {
		go d.sshClient.Close()
		d.sshClient = nil
	}
}

// NewSSHOverWSTransport creates an http.Transport that routes through a
// remote SSH server via a WebSocket-over-TLS relay. It works like
// "ssh -ND" but without binding a local port — SSH direct-tcpip channels
// handle the proxying from the remote side.
//
//   - wsRelayHost: the WebSocket relay server (e.g. "websocket-tcp-proxy.dev")
//   - sshHost:     the SSH server hostname
//   - sshPort:     the SSH server port
//   - sshUser:     the SSH username
//   - sshPass:     the SSH password
func NewSSHOverWSTransport(wsRelayHost, sshHost, sshPort, sshUser, sshPass string) (*http.Transport, error) {
	dialer := &sshOverWSDialer{
		config: sshOverWSConfig{
			wsRelayHost: wsRelayHost,
			sshHost:     sshHost,
			sshPort:     sshPort,
			sshUser:     sshUser,
			sshPass:     sshPass,
		},
	}

	return &http.Transport{
		DialContext:     dialer.DialContext,
		TLSClientConfig: insecure,
	}, nil
}
