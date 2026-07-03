package adstest

import (
	"bytes"
	"net"
	"strconv"

	"espx/internal/ads"

	"github.com/panjf2000/gnet/v2"
)

var gnetHarnessRemoteAddr = &net.TCPAddr{IP: net.IPv4(1, 1, 1, 1), Port: 1234}

// GnetHarnessConn is a minimal gnet.Conn stub for OnTraffic tests.
type GnetHarnessConn struct {
	gnet.Conn
	inbound []byte
	written []byte
	ctx     any
	addr    net.Addr
}

// NewGnetHarnessConn returns a connection preloaded with raw HTTP request bytes.
func NewGnetHarnessConn(inbound []byte) *GnetHarnessConn {
	return &GnetHarnessConn{
		inbound: append([]byte(nil), inbound...),
		written: make([]byte, 0, 512),
		addr:    gnetHarnessRemoteAddr,
	}
}

func (harnessConn *GnetHarnessConn) Context() any { return harnessConn.ctx }

func (harnessConn *GnetHarnessConn) SetContext(v any) { harnessConn.ctx = v }

func (harnessConn *GnetHarnessConn) Write(b []byte) (int, error) {
	harnessConn.written = append(harnessConn.written[:0], b...)
	return len(b), nil
}

func (harnessConn *GnetHarnessConn) AsyncWrite(buf []byte, callback gnet.AsyncCallback) error {
	harnessConn.written = append(harnessConn.written[:0], buf...)
	if callback != nil {
		_ = callback(harnessConn, nil)
	}
	return nil
}

func (harnessConn *GnetHarnessConn) InboundBuffered() int { return len(harnessConn.inbound) }

func (harnessConn *GnetHarnessConn) Peek(n int) ([]byte, error) {
	if n > len(harnessConn.inbound) {
		n = len(harnessConn.inbound)
	}
	return harnessConn.inbound[:n], nil
}

func (harnessConn *GnetHarnessConn) Discard(n int) (int, error) {
	if n > len(harnessConn.inbound) {
		n = len(harnessConn.inbound)
	}
	harnessConn.inbound = harnessConn.inbound[n:]
	return n, nil
}

func (harnessConn *GnetHarnessConn) RemoteAddr() net.Addr {
	if harnessConn.addr != nil {
		return harnessConn.addr
	}
	return gnetHarnessRemoteAddr
}

// Written returns the last response bytes written to the connection.
func (harnessConn *GnetHarnessConn) Written() []byte { return harnessConn.written }

// SetRemoteAddr overrides the peer address for proxy header tests.
func (harnessConn *GnetHarnessConn) SetRemoteAddr(addr net.Addr) { harnessConn.addr = addr }

// BuildGnetHTTP assembles a minimal HTTP/1.1 request with optional headers and body.
func BuildGnetHTTP(method, path string, headers map[string]string, body []byte) []byte {
	var buf bytes.Buffer
	buf.WriteString(method)
	buf.WriteByte(' ')
	buf.WriteString(path)
	buf.WriteString(" HTTP/1.1\r\n")
	if _, ok := headers["Content-Length"]; !ok && body != nil {
		headers = copyGnetHarnessHeaders(headers)
		headers["Content-Length"] = strconv.Itoa(len(body))
	}
	for k, v := range headers {
		buf.WriteString(k)
		buf.WriteString(": ")
		buf.WriteString(v)
		buf.WriteString("\r\n")
	}
	buf.WriteString("\r\n")
	if len(body) > 0 {
		buf.Write(body)
	}
	return buf.Bytes()
}

func copyGnetHarnessHeaders(h map[string]string) map[string]string {
	out := make(map[string]string, len(h)+1)
	for k, v := range h {
		out[k] = v
	}
	return out
}

// BuildGnetPostTrackJSON wraps a JSON body in POST /track HTTP bytes.
func BuildGnetPostTrackJSON(body []byte) []byte {
	return BuildGnetHTTP("POST", "/track", map[string]string{
		"Content-Type": "application/json",
		"Connection":   "keep-alive",
	}, body)
}

// BuildGnetGetHealth builds GET /health request bytes.
func BuildGnetGetHealth() []byte {
	return BuildGnetHTTP("GET", "/health", map[string]string{
		"Connection":     "keep-alive",
		"Content-Length": "0",
	}, nil)
}

// ServeGnetHarness runs OnTraffic against raw HTTP bytes.
func ServeGnetHarness(h *ads.AdsPacketHandler, inbound []byte) (gnet.Action, *GnetHarnessConn) {
	c := NewGnetHarnessConn(inbound)
	return h.OnTraffic(c), c
}

// ParseGnetHTTPStatus extracts the numeric HTTP status from a raw HTTP response.
func ParseGnetHTTPStatus(resp []byte) int {
	if len(resp) < 12 || !bytes.HasPrefix(resp, []byte("HTTP/1.1 ")) {
		return 0
	}
	code := 0
	for i := 9; i < len(resp) && resp[i] != ' '; i++ {
		if resp[i] >= '0' && resp[i] <= '9' {
			code = code*10 + int(resp[i]-'0')
		} else {
			break
		}
	}
	return code
}

// ParseGnetHTTPBody returns the response body after the header block.
func ParseGnetHTTPBody(resp []byte) []byte {
	idx := bytes.Index(resp, []byte("\r\n\r\n"))
	if idx < 0 {
		return nil
	}
	return resp[idx+4:]
}

// PostTrackGnet sends POST /track through OnTraffic.
func PostTrackGnet(h *ads.AdsPacketHandler, body []byte, contentType, accept string) (int, []byte) {
	headers := map[string]string{
		"Content-Type": contentType,
		"Connection":   "keep-alive",
	}
	if accept != "" {
		headers["Accept"] = accept
	}
	_, conn := ServeGnetHarness(h, BuildGnetHTTP("POST", "/track", headers, body))
	return ParseGnetHTTPStatus(conn.Written()), conn.Written()
}

// PostTrackGnetJSON is a shortcut for JSON track requests.
func PostTrackGnetJSON(h *ads.AdsPacketHandler, body []byte) (int, []byte) {
	_, conn := ServeGnetHarness(h, BuildGnetPostTrackJSON(body))
	return ParseGnetHTTPStatus(conn.Written()), conn.Written()
}

// GetHealthGnet sends GET /health through OnTraffic.
func GetHealthGnet(h *ads.AdsPacketHandler) (status int, body string) {
	_, conn := ServeGnetHarness(h, BuildGnetGetHealth())
	return ParseGnetHTTPStatus(conn.Written()), string(ParseGnetHTTPBody(conn.Written()))
}
