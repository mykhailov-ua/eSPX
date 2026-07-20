package postback

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWebhookAdapter_CachesParsedTemplate(t *testing.T) {
	a := &WebhookAdapter{}
	url := "https://track.example/postback?clickid={click_id}&amt={payout}"
	mt1 := a.cachedTemplate(url)
	mt2 := a.cachedTemplate(url)
	if mt1 != mt2 {
		t.Fatal("expected same cached MacroTemplate pointer")
	}
}

func TestWebhookAdapter_Send_UsesMacros(t *testing.T) {
	var gotURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.String()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tpl := srv.URL + "/postback?clickid={click_id}&amt={payout}"
	a := &WebhookAdapter{}
	err := a.Send(context.Background(), srv.Client(), &PostbackPayload{
		ClickID: "abc",
		Payout:  10.5,
	}, tpl, "")
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	want := "/postback?clickid=abc&amt=10.5"
	if gotURL != want {
		t.Fatalf("got URL path %q, want %q", gotURL, want)
	}
}

func TestWebhookAdapter_RenderPath_ZeroAllocAfterCache(t *testing.T) {
	a := &WebhookAdapter{}
	tpl := "https://track.example/postback?clickid={click_id}&amt={payout}&tx={tx_id}"
	mt := a.cachedTemplate(tpl)
	ctx := &EventContext{ClickID: "click123", Payout: "10.50", TxID: "tx999"}
	var scratch [MaxRenderedURLLen]byte

	allocs := testing.AllocsPerRun(1000, func() {
		_ = mt.RenderStack(ctx, &scratch)
	})
	if allocs != 0 {
		t.Fatalf("RenderStack allocs/op = %v, want 0", allocs)
	}
}
