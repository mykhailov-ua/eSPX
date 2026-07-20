package postback

import (
	"testing"
)

func TestMacroSubstitution(t *testing.T) {
	tests := []struct {
		name     string
		template string
		ctx      EventContext
		expected string
	}{
		{
			name:     "basic webhook url",
			template: "https://track.affiliate.com/postback?clickid={click_id}&amt={payout}&tx={tx_id}",
			ctx: EventContext{
				ClickID: "click123",
				Payout:  "10.50",
				TxID:    "tx999",
			},
			expected: "https://track.affiliate.com/postback?clickid=click123&amt=10.50&tx=tx999",
		},
		{
			name:     "all macros and static text",
			template: "http://example.com/{click_id}/{payout}/{tx_id}/{subid1}/{param10}/{event_type}",
			ctx: EventContext{
				ClickID:   "c",
				Payout:    "p",
				TxID:      "t",
				SubID1:    "s",
				Param10:   "p10",
				EventType: "et",
			},
			expected: "http://example.com/c/p/t/s/p10/et",
		},
		{
			name:     "case insensitive macros",
			template: "http://example.com?click={CLICK_id}&val={PAYOUT}",
			ctx: EventContext{
				ClickID: "abc",
				Payout:  "5",
			},
			expected: "http://example.com?click=abc&val=5",
		},
		{
			name:     "unknown macro handled as static",
			template: "http://example.com?click={click_id}&extra={unknown_macro}",
			ctx: EventContext{
				ClickID: "abc",
			},
			expected: "http://example.com?click=abc&extra={unknown_macro}",
		},
		{
			name:     "unmatched bracket handled as static",
			template: "http://example.com?click={click_id",
			ctx: EventContext{
				ClickID: "abc",
			},
			expected: "http://example.com?click={click_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mt := ParseTemplate(tt.template)
			var scratch [MaxRenderedURLLen]byte
			got := mt.RenderStack(&tt.ctx, &scratch)
			if string(got) != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, string(got))
			}
		})
	}
}

func TestMacroRender_ZeroAlloc(t *testing.T) {
	mt := ParseTemplate("https://track.affiliate.com/postback?clickid={click_id}&amt={payout}&tx={tx_id}")
	ctx := EventContext{ClickID: "click123", Payout: "10.50", TxID: "tx999"}
	var scratch [MaxRenderedURLLen]byte

	allocs := testing.AllocsPerRun(1000, func() {
		_ = mt.RenderStack(&ctx, &scratch)
	})
	if allocs != 0 {
		t.Fatalf("RenderStack allocs/op = %v, want 0", allocs)
	}
}

func TestParseTemplate_AllocBudget(t *testing.T) {
	tpl := "https://track.affiliate.com/postback?clickid={click_id}&amt={payout}&tx={tx_id}&sub={subid1}&p10={param10}&et={event_type}"
	allocs := testing.AllocsPerRun(100, func() {
		_ = ParseTemplate(tpl)
	})
	// Cold path: MacroTemplate struct + owned slab (2 allocs), no per-macro ToLower.
	if allocs > 2 {
		t.Fatalf("ParseTemplate allocs/op = %v, want <= 2", allocs)
	}
}
