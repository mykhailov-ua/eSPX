package ingestion

import (
	"bytes"
	"net"
	"strconv"
	"time"

	"github.com/panjf2000/gnet/v2"
)

// gnetHarnessRemoteAddr is the default peer for harness connections.
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

func (c *GnetHarnessConn) Context() any     { return c.ctx }
func (c *GnetHarnessConn) SetContext(v any) { c.ctx = v }

func (c *GnetHarnessConn) Write(b []byte) (int, error) {
	c.written = append(c.written[:0], b...)
	return len(b), nil
}

func (c *GnetHarnessConn) AsyncWrite(buf []byte, callback gnet.AsyncCallback) error {
	c.written = append(c.written[:0], buf...)
	if callback != nil {
		_ = callback(c, nil)
	}
	return nil
}

func (c *GnetHarnessConn) InboundBuffered() int { return len(c.inbound) }

func (c *GnetHarnessConn) Peek(n int) ([]byte, error) {
	if n > len(c.inbound) {
		n = len(c.inbound)
	}
	return c.inbound[:n], nil
}

func (c *GnetHarnessConn) Discard(n int) (int, error) {
	if n > len(c.inbound) {
		n = len(c.inbound)
	}
	c.inbound = c.inbound[n:]
	return n, nil
}

func (c *GnetHarnessConn) RemoteAddr() net.Addr {
	if c.addr != nil {
		return c.addr
	}
	return gnetHarnessRemoteAddr
}

// Written returns the last response bytes written to the connection.
func (c *GnetHarnessConn) Written() []byte { return c.written }

// SetRemoteAddr overrides the peer address for proxy header tests.
func (c *GnetHarnessConn) SetRemoteAddr(addr net.Addr) { c.addr = addr }

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
func ServeGnetHarness(h *AdsPacketHandler, inbound []byte) (gnet.Action, *GnetHarnessConn) {
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
func PostTrackGnet(h *AdsPacketHandler, body []byte, contentType, accept string) (int, []byte) {
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
func PostTrackGnetJSON(h *AdsPacketHandler, body []byte) (int, []byte) {
	return PostTrackGnetJSONWait(h, body, 0)
}

// PostTrackGnetJSONWait runs POST /track and polls until the worker pool writes a response or timeout.
// timeout 0 uses 5s. Required for concurrent chaos load tests using the async gnet worker pool.
func PostTrackGnetJSONWait(h *AdsPacketHandler, body []byte, timeout time.Duration) (int, []byte) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	_, conn := ServeGnetHarness(h, BuildGnetPostTrackJSON(body))
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		written := conn.Written()
		if len(written) > 0 {
			return ParseGnetHTTPStatus(written), written
		}
		time.Sleep(50 * time.Microsecond)
	}
	return 0, nil
}

// GetHealthGnet sends GET /health through OnTraffic.
func GetHealthGnet(h *AdsPacketHandler) (status int, body string) {
	_, conn := ServeGnetHarness(h, BuildGnetGetHealth())
	return ParseGnetHTTPStatus(conn.Written()), string(ParseGnetHTTPBody(conn.Written()))
}
