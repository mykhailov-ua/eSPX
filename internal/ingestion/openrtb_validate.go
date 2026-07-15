package ingestion

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

const maxOpenRTBValidationErrors = 50

var allowedBidCurrencies = map[string]struct{}{
	"USD": {},
	"EUR": {},
}

// OpenRTBValidationResultDTO is the admin lint response for bid request payloads.
type OpenRTBValidationResultDTO struct {
	Valid   bool     `json:"valid"`
	Version string   `json:"version,omitempty"`
	Errors  []string `json:"errors"`
}

type validationCollector struct {
	errors []string
}

func (c *validationCollector) add(format string, args ...any) {
	if len(c.errors) >= maxOpenRTBValidationErrors {
		return
	}
	c.errors = append(c.errors, fmt.Sprintf(format, args...))
}

func (c *validationCollector) result(version string) OpenRTBValidationResultDTO {
	return OpenRTBValidationResultDTO{
		Valid:   len(c.errors) == 0,
		Version: version,
		Errors:  c.errors,
	}
}

// ValidateOpenRTBBidRequest lints OpenRTB 2.6 or 3.0 bid request JSON (cold path).
func ValidateOpenRTBBidRequest(payload []byte) OpenRTBValidationResultDTO {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 {
		var c validationCollector
		c.add("request body is empty")
		return c.result("")
	}
	if !json.Valid(payload) {
		var c validationCollector
		c.add("invalid JSON")
		return c.result("")
	}
	if isOpenRTB30Payload(payload) {
		return validateOpenRTB30(payload)
	}
	return validateOpenRTB26(payload)
}

func isOpenRTB30Payload(payload []byte) bool {
	_, _, _, isOpenRTB := ParseOpenRTB3Payload(payload)
	return isOpenRTB
}

type bidRequest26 struct {
	ID   string          `json:"id"`
	Imp  []impObject26   `json:"imp"`
	Site json.RawMessage `json:"site"`
	App  json.RawMessage `json:"app"`
	DOOH json.RawMessage `json:"dooh"`
	Cur  []string        `json:"cur"`
}

type impObject26 struct {
	ID          string          `json:"id"`
	Banner      json.RawMessage `json:"banner"`
	Video       json.RawMessage `json:"video"`
	Audio       json.RawMessage `json:"audio"`
	Native      json.RawMessage `json:"native"`
	Bidfloor    *float64        `json:"bidfloor"`
	Bidfloorcur string          `json:"bidfloorcur"`
}

func validateOpenRTB26(payload []byte) OpenRTBValidationResultDTO {
	var c validationCollector
	var req bidRequest26
	if err := json.Unmarshal(payload, &req); err != nil {
		c.add("invalid OpenRTB 2.6 JSON: %v", err)
		return c.result("2.6")
	}

	if strings.TrimSpace(req.ID) == "" {
		c.add("BidRequest.id is required (OpenRTB 2.6 sec 3.2.1)")
	}
	if len(req.Imp) == 0 {
		c.add("BidRequest.imp must contain at least one Imp object (OpenRTB 2.6 sec 3.2.1)")
	}

	inventoryCount := 0
	if len(req.Site) > 0 && string(req.Site) != "null" {
		inventoryCount++
	}
	if len(req.App) > 0 && string(req.App) != "null" {
		inventoryCount++
	}
	if len(req.DOOH) > 0 && string(req.DOOH) != "null" {
		inventoryCount++
	}
	if inventoryCount > 1 {
		c.add("BidRequest must contain at most one of site, app, or dooh (OpenRTB 2.6 sec 3.2.13-15)")
	}

	validateCurrencyList(&c, req.Cur, "BidRequest.cur")

	for i, imp := range req.Imp {
		prefix := fmt.Sprintf("imp[%d]", i)
		if strings.TrimSpace(imp.ID) == "" {
			c.add("%s.id is required (OpenRTB 2.6 sec 3.2.4)", prefix)
		}
		if !impHasAdFormat(imp) {
			c.add("%s must include at least one of banner, video, audio, or native (OpenRTB 2.6 sec 3.2.4)", prefix)
		}
		if imp.Bidfloor != nil && *imp.Bidfloor < 0 {
			c.add("%s.bidfloor must be non-negative", prefix)
		}
		if cur := strings.TrimSpace(imp.Bidfloorcur); cur != "" {
			validateCurrencyCode(&c, cur, prefix+".bidfloorcur")
		}
	}

	return c.result("2.6")
}

func impHasAdFormat(imp impObject26) bool {
	return isPresentJSON(imp.Banner) || isPresentJSON(imp.Video) ||
		isPresentJSON(imp.Audio) || isPresentJSON(imp.Native)
}

func isPresentJSON(raw json.RawMessage) bool {
	if len(raw) == 0 || string(raw) == "null" {
		return false
	}
	return true
}

type openRTB30Envelope struct {
	OpenRTB openRTB30Root `json:"openrtb"`
}

type openRTB30Root struct {
	Ver     string       `json:"ver"`
	Request openRTB30Req `json:"request"`
}

type openRTB30Req struct {
	ID   string          `json:"id"`
	Cur  []string        `json:"cur"`
	Item []openRTB30Item `json:"item"`
}

type openRTB30Item struct {
	ID  string   `json:"id"`
	Flr *float64 `json:"flr"`
}

func validateOpenRTB30(payload []byte) OpenRTBValidationResultDTO {
	var c validationCollector

	minBid, deviceType, categoryMask, isOpenRTB := ParseOpenRTB3Payload(payload)
	if !isOpenRTB {
		c.add("payload is not a recognizable OpenRTB 3.0 document")
		return c.result("3.0")
	}

	var env openRTB30Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		c.add("invalid OpenRTB 3.0 JSON: %v", err)
		return c.result("3.0")
	}

	req := env.OpenRTB.Request
	if strings.TrimSpace(req.ID) == "" {
		c.add("openrtb.request.id is required (OpenRTB 3.0 sec 4.2.1)")
	}
	if len(req.Item) == 0 {
		c.add("openrtb.request.item must contain at least one Item object (OpenRTB 3.0 sec 4.2.1)")
	}
	for i, item := range req.Item {
		if strings.TrimSpace(item.ID) == "" {
			c.add("openrtb.request.item[%d].id is required (OpenRTB 3.0 sec 4.2.3)", i)
		}
		if item.Flr != nil && *item.Flr < 0 {
			c.add("openrtb.request.item[%d].flr must be non-negative", i)
		}
	}

	validateCurrencyList(&c, req.Cur, "openrtb.request.cur")

	// Cross-check hot-path parser outputs for fields the tracker consumes.
	if bytes.Contains(payload, []byte(`"flr"`)) && minBid == 0 {
		c.add("openrtb.request.item flr present but could not be parsed (expected decimal CPM)")
	}
	if bytes.Contains(payload, []byte(`"device"`)) && bytes.Contains(payload, []byte(`"type"`)) && deviceType == 0 {
		c.add("openrtb.request.context.device.type present but could not be mapped")
	}
	if bytes.Contains(payload, []byte(`"category_mask"`)) && categoryMask == 0 {
		c.add("category_mask extension present but could not be parsed")
	}

	return c.result("3.0")
}

func validateCurrencyList(c *validationCollector, currencies []string, field string) {
	if len(currencies) == 0 {
		return
	}
	for i, cur := range currencies {
		validateCurrencyCode(c, cur, fmt.Sprintf("%s[%d]", field, i))
	}
}

func validateCurrencyCode(c *validationCollector, cur, field string) {
	cur = strings.ToUpper(strings.TrimSpace(cur))
	if cur == "" {
		return
	}
	if _, ok := allowedBidCurrencies[cur]; !ok {
		c.add("%s currency %q is not allowed; only USD and EUR are accepted", field, cur)
	}
}
