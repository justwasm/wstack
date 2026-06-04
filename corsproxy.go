package wstack

import (
	"net/http"
	"net/url"
)

// BaseTransport is the original http.DefaultTransport saved before any
// wrapping. Tools that clone the transport should use this.
var BaseTransport = http.DefaultTransport.(*http.Transport)

// corsTransport rewrites the request URL through the CORS proxy prefix.
type corsTransport struct {
	prefixURL *url.URL
}

func (t *corsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	proxyReq := req.Clone(req.Context())
	proxyReq.URL = t.prefixURL.JoinPath(req.URL.String())

	return BaseTransport.RoundTrip(proxyReq)
}

// NewCorsProxyTransport returns an http.RoundTripper that rewrites all
// requests through the given CORS proxy URL prefix.
//
//	transport, err := NewCorsProxyTransport("https://no-cors.deno.dev/")
//	client := &http.Client{Transport: transport}
func NewCorsProxyTransport(proxyURL string) (http.RoundTripper, error) {
	u, err := url.Parse(proxyURL)
	if err != nil {
		return nil, err
	}
	return &corsTransport{prefixURL: u}, nil
}
