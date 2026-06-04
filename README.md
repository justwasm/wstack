# wstack

**wasm-friendly userland network stack over WebSocket**

`wstack` is a Go library that lets WebAssembly (and native) programs make TCP and UDP connections by tunneling IP packets over a WebSocket connection to a server-side proxy. The entire TCP/IP stack runs in userspace (powered by [gVisor](https://gvisor.dev/)), so it works inside the browser's WebAssembly sandbox.

---

## Architecture

```
WASM client                                    Proxy server (wsproxy)
──────────────────────────────────────────     ─────────────────────────────────────────
Application (net.Conn / net.Listener)          TCP / UDP forwarder
         ↓                                              ↓
 gVisor userspace TCP/IP stack            gVisor userspace TCP/IP stack
 (AllowExternalLoopbackTraffic)                         ↓
         ↓  raw IP frames                     channel.Endpoint (per-client)
  channel.Endpoint                                      ↑
         ↓                                     WebSocket upgrade handler
  WebSocket (nhooyr.io/websocket)     ←────────────────┘
```

Raw IPv4/IPv6 frames are exchanged as binary WebSocket messages.  
The proxy creates an isolated gVisor stack per client and uses TCP/UDP forwarders to proxy each connection to its real destination on the internet.

---

## Installation

```bash
go get github.com/justwasm/wstack
```

---

## Quick start

### 1 — Start the proxy server

```bash
go install github.com/justwasm/wstack/cmd/wsproxy@latest
wsproxy -addr :8080 -path /ws
```

Or build it yourself:

```bash
git clone https://github.com/justwasm/wstack
cd wstack
go build -o wsproxy ./cmd/wsproxy
./wsproxy -addr :8080 -path /ws
```

### 2 — Use the client library

```go
package main

import (
    "context"
    "io"
    "log"
    "net/netip"

    "github.com/justwasm/wstack"
)

func main() {
    ctx := context.Background()

    // Connect to the proxy and create a virtual network stack.
    s, err := wstack.New(ctx, "ws://localhost:8080/ws", wstack.Options{
        LocalAddrs: []netip.Addr{netip.MustParseAddr("10.0.0.2")},
    })
    if err != nil {
        log.Fatal(err)
    }
    defer s.Close()

    // Dial TCP through the proxy — works exactly like net.Dial.
    conn, err := s.Dial(ctx, "tcp", "example.com:80")
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    io.WriteString(conn, "GET / HTTP/1.0\r\nHost: example.com\r\n\r\n")
    io.Copy(io.Discard, conn)
}
```

### 3 — Compile for WebAssembly

```bash
GOOS=js GOARCH=wasm go build -o app.wasm .
```

Use the standard `wasm_exec.js` runtime (shipped with your Go installation) to load the binary in a browser or Node.js.

---

## API reference

### `wstack.New(ctx, wsURL, opts) (*Stack, error)`

Creates a new `Stack` connected to the WebSocket proxy at `wsURL`.

| Option field | Default | Description |
|---|---|---|
| `LocalAddrs` | `[10.0.0.2]` | IP addresses assigned to the virtual NIC |
| `MTU` | `1420` | Maximum transmission unit |
| `DialOptions` | `nil` | Extra options for the underlying WebSocket dial |

### `(*Stack).Dial(ctx, network, address) (net.Conn, error)`

Dials `address` through the proxy. Supported networks: `tcp`, `tcp4`, `tcp6`, `udp`, `udp4`, `udp6`.

### `(*Stack).Listen(network, address) (net.Listener, error)`

Listens on `address` within the virtual stack. Supported networks: `tcp`, `tcp4`, `tcp6`.

### `(*Stack).GStack() *stack.Stack`

Returns the underlying gVisor `*stack.Stack` for advanced use.

### `(*Stack).Close() error`

Shuts down the stack and the WebSocket connection.

---

## `wsproxy` server flags

| Flag | Default | Description |
|---|---|---|
| `-addr` | `:8080` | TCP address to listen on |
| `-path` | `/ws` | HTTP path to handle WebSocket upgrades |

---

## Security notes

* **The proxy has no authentication by default.** Restrict access with a reverse proxy (nginx, Caddy) or add token-based auth middleware before deploying to production.
* The proxy forwards to **any** TCP/UDP destination reachable from the server. Apply firewall rules or an egress allowlist as needed.

---

## License

MIT — see [LICENSE](LICENSE).

