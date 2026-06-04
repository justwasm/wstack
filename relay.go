package wstack

import "net/url"

// relayURL builds a WebSocket relay URL with hostname & port as query params.
// Example:
//
//	relayURL("https://websocket-tcp-proxy.dev", "socks5.com", "1080")
//	→ "wss://websocket-tcp-proxy.dev?hostname=socks5.com&port=1080"
func relayURL(relayHost, targetHost, targetPort string) *url.URL {
	u, err := url.Parse(relayHost)
	if err != nil {
		// wsRelayHost is a static value set at compile time, guaranteed valid.
		panic("invalid relay host: " + err.Error())
	}
	u.RawQuery = url.Values{
		"hostname": {targetHost},
		"port":     {targetPort},
	}.Encode()
	return u
}
