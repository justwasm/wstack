package wstack

import (
	"io"
	"net/http"
	"testing"
	"time"
)

var (
	sshWSRelayHost = "https://websocket-tcp-proxy.up.railway.app"
	sshTargetHost  = "pwnable.kr"
	sshTargetPort  = "2222"
	sshTargetUser  = "col"
	sshTargetPass  = "guest"

	sshTestTargetURL = "https://ip.sb"
)

func TestSSH(t *testing.T) {
	transport, err := NewSSHOverWSTransport(sshWSRelayHost, sshTargetHost, sshTargetPort, sshTargetUser, sshTargetPass)
	if err != nil {
		t.Fatalf("NewSSHOverWSTransport: %v", err)
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	resp, err := client.Get(sshTestTargetURL)
	if err != nil {
		t.Fatalf("SSH request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	t.Logf("SSH proxy OK: %s", string(body))
}
