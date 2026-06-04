package wstack

import (
	"bufio"
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestYamuxWorkerSSE(t *testing.T) {
	transport, err := NewYamuxOverWSTransport("https://yamux-worker.navigaid.workers.dev")
	if err != nil {
		t.Fatalf("NewYamuxOverWSTransport: %v", err)
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   15 * time.Second,
	}

	resp, err := client.Get("http://httpbin.org/stream/3")
	if err != nil {
		t.Fatalf("SSE request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %s", resp.Status)
	}

	scanner := bufio.NewScanner(resp.Body)
	lines := 0
	for scanner.Scan() {
		_ = scanner.Text()
		if lines < 3 {
			lines++
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read error: %v", err)
	}
	if lines == 0 {
		t.Fatal("no SSE data received")
	}
}

func TestYamuxWorkerWebSocket(t *testing.T) {
	wsURL, err := url.Parse("https://yamux-worker.navigaid.workers.dev")
	if err != nil {
		t.Fatalf("yamuxWSURL: %v", err)
	}
	yamuxDialer := &yamuxOverWSDialer{
		wsURL: wsURL,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, "wss://ws.postman-echo.com/raw", &websocket.DialOptions{
		HTTPClient: &http.Client{
			Transport: &http.Transport{
				DialContext: yamuxDialer.DialContext,
			},
		},
	})
	if err != nil {
		t.Fatalf("WebSocket dial failed: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	err = conn.Write(ctx, websocket.MessageText, []byte("hello yamux"))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	_, msg, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}

	if string(msg) != "hello yamux" {
		t.Fatalf("expected 'hello yamux', got '%s'", string(msg))
	}
	t.Logf("WebSocket echo OK: %s", string(msg))
}
