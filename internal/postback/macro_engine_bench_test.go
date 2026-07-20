package postback

import "testing"

var (
	benchTemplate = "https://track.affiliate.com/postback?clickid={click_id}&amt={payout}&tx={tx_id}&sub={subid1}&p10={param10}&et={event_type}"
	benchParsed   = ParseTemplate(benchTemplate)
	benchCtx      = EventContext{
		ClickID:   "click123456789",
		Payout:    "10.50",
		TxID:      "tx999",
		SubID1:    "sub1",
		Param10:   "keyword",
		EventType: "conversion",
	}
)

func BenchmarkParseTemplate(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = ParseTemplate(benchTemplate)
	}
}

func BenchmarkMacroRenderAppend_Stack(b *testing.B) {
	b.ReportAllocs()
	var scratch [MaxRenderedURLLen]byte
	for i := 0; i < b.N; i++ {
		_ = benchParsed.RenderStack(&benchCtx, &scratch)
	}
}

func BenchmarkMacroRenderAppend_ReusedSlice(b *testing.B) {
	b.ReportAllocs()
	buf := make([]byte, 0, 256)
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		_ = benchParsed.RenderAppend(buf, &benchCtx)
	}
}
