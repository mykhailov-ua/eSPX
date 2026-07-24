package ingestion

import (
	"bytes"
	"testing"
)

// http1HappyCorpus — POST /track fast-path request line + Content-Length only.
var http1HappyCorpus = []byte(
	"POST /track HTTP/1.1\r\n" +
		"Content-Length: 69\r\n" +
		"\r\n" +
		`{"campaign_id":"00000000-0000-0000-0000-000000000001","type":"click"}`,
)

// http1WorstCorpus — full nginx edge header set (M5-B benchmark corpus).
var http1WorstCorpus = nginxTrackCorpus

func BenchmarkHTTP1DFA_Happy(b *testing.B) {
	const maxBody = int64(1024 * 1024)
	b.SetBytes(int64(len(http1HappyCorpus)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := parseHTTP1(http1HappyCorpus, maxBody)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHTTP1DFA_Worst(b *testing.B) {
	const maxBody = int64(1024 * 1024)
	b.SetBytes(int64(len(http1WorstCorpus)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := parseHTTP1(http1WorstCorpus, maxBody)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHTTP2DFA_Happy(b *testing.B) {
	buf := []byte{0x00, 0x00, 0x05, h2FrameData, 0x00, 0x00, 0x00, 0x00, 0x01, 'h', 'e', 'l', 'l', 'o'}
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := decodeH2FrameHeader(buf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHTTP2DFA_Worst(b *testing.B) {
	body := []byte(`{"campaign_id":"00000000-0000-0000-0000-000000000001","type":"click"}`)
	wire := buildH2TrackRequest(body)
	b.SetBytes(int64(len(wire)))
	b.ReportAllocs()
	st := newH2ConnState()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st.resetConn()
		_, _, _, _, err := parseH2Ingress(wire, &st, 1<<20)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHTTP3DFA_Happy(b *testing.B) {
	buf := []byte{0x25} // 1-byte QUIC varint (37)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := quicDecodeVarint(buf, 0)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHTTP3DFA_Worst(b *testing.B) {
	body := []byte(`{"campaign_id":"00000000-0000-0000-0000-000000000001","type":"click"}`)
	var hdrBlock []byte
	hdrBlock = append(hdrBlock, 0x83, 0x04, 0x06, '/', 't', 'r', 'a', 'c', 'k')
	var buf bytes.Buffer
	writeHTTP3Frame(&buf, h3FrameHeaders, hdrBlock)
	writeHTTP3Frame(&buf, h3FrameData, body)
	wire := buf.Bytes()
	b.SetBytes(int64(len(wire)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := h3ParseRequestFrames(wire, 1<<20)
		if err != nil {
			b.Fatal(err)
		}
	}
}
