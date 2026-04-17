package vmess

import (
	"bufio"
	"context"
	"io"
	"net"
	"strings"
	"testing"

	N "github.com/metacubex/mihomo/common/net"

	"github.com/metacubex/http"
)

func TestStreamWebsocketConnSetsBrowserUserAgent(t *testing.T) {
	headers := captureWebSocketHandshakeHeaders(t, http.Header{})
	userAgent := headers.Get("User-Agent")

	if userAgent == "" {
		t.Fatal("expected websocket handshake to include User-Agent")
	}
	if userAgent == "Go-http-client/1.1" {
		t.Fatalf("expected browser User-Agent, got %q", userAgent)
	}
	if !strings.HasPrefix(userAgent, "Mozilla/5.0") {
		t.Fatalf("expected browser User-Agent, got %q", userAgent)
	}
}

func TestStreamWebsocketConnPreservesExplicitUserAgent(t *testing.T) {
	const expectedUserAgent = "CustomAgent/1.0"

	headers := http.Header{}
	headers.Set("User-Agent", expectedUserAgent)

	handshakeHeaders := captureWebSocketHandshakeHeaders(t, headers)
	if got := handshakeHeaders.Get("User-Agent"); got != expectedUserAgent {
		t.Fatalf("expected User-Agent %q, got %q", expectedUserAgent, got)
	}
}

func captureWebSocketHandshakeHeaders(t *testing.T, requestHeaders http.Header) http.Header {
	t.Helper()

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	headersCh := make(chan http.Header, 1)
	serverErrCh := make(chan error, 1)

	go func() {
		req, err := http.ReadRequest(bufio.NewReader(serverConn))
		if err != nil {
			serverErrCh <- err
			return
		}
		headersCh <- req.Header.Clone()

		if req.Body != nil {
			_, _ = io.Copy(io.Discard, req.Body)
			_ = req.Body.Close()
		}

		secAccept := N.GetWebSocketSecAccept(req.Header.Get("Sec-WebSocket-Key"))
		_, err = io.WriteString(serverConn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: "+secAccept+"\r\n\r\n")
		serverErrCh <- err
	}()

	conn, err := streamWebsocketConn(context.Background(), clientConn, &WebsocketConfig{
		Host:    "example.com",
		Port:    "80",
		Path:    "/ws",
		Headers: requestHeaders.Clone(),
	}, nil)
	if err != nil {
		t.Fatalf("streamWebsocketConn failed: %v", err)
	}
	_ = conn.Close()

	headers := <-headersCh
	if err := <-serverErrCh; err != nil {
		t.Fatalf("websocket server handshake failed: %v", err)
	}
	return headers
}
