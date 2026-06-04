package wstack

import (
	"net/http"
	"testing"
	"time"
)

func TestNewCorsProxyTransport(t *testing.T) {
	transport, err := NewCorsProxyTransport("https://no-cors.deno.dev/")
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   15 * time.Second,
	}

	resp, err := client.Get("https://httpbin.org/get")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %s", resp.Status)
	}
}

func TestNewCorsProxyTransportPost(t *testing.T) {
	transport, err := NewCorsProxyTransport("https://no-cors.deno.dev/")
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   15 * time.Second,
	}

	resp, err := client.Post("https://httpbin.org/post", "text/plain", nil)
	if err != nil {
		t.Fatalf("POST request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %s", resp.Status)
	}
}

func TestNewCorsProxyTransportInvalidURL(t *testing.T) {
	_, err := NewCorsProxyTransport("://invalid")
	if err == nil {
		t.Fatal("expected error for invalid proxy URL")
	}
}
