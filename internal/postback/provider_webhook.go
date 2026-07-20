package postback

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
)

type WebhookAdapter struct {
	templates sync.Map // url template string -> *MacroTemplate
}

func (a *WebhookAdapter) cachedTemplate(urlTemplate string) *MacroTemplate {
	if v, ok := a.templates.Load(urlTemplate); ok {
		return v.(*MacroTemplate)
	}
	mt := ParseTemplate(urlTemplate)
	if v, loaded := a.templates.LoadOrStore(urlTemplate, mt); loaded {
		return v.(*MacroTemplate)
	}
	return mt
}

func (a *WebhookAdapter) Send(ctx context.Context, client *http.Client, payload *PostbackPayload, urlTemplate string, apiTokenDecrypted string) error {
	mt := a.cachedTemplate(urlTemplate)
	evtCtx := &EventContext{
		ClickID:   payload.ClickID,
		Payout:    strconv.FormatFloat(payload.Payout, 'f', -1, 64),
		TxID:      payload.TxID,
		SubID1:    payload.SubID1,
		Param10:   payload.Param10,
		EventType: payload.EventType,
	}
	var scratch [MaxRenderedURLLen]byte
	renderedURL := string(mt.RenderStack(evtCtx, &scratch))

	req, err := http.NewRequestWithContext(ctx, "GET", renderedURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	if apiTokenDecrypted != "" {
		req.Header.Set("Authorization", "Bearer "+apiTokenDecrypted)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
