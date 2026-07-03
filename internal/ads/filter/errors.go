package filter

import (
	"context"
	"errors"
	"net/http"

	"espx/internal/database"
	"espx/internal/metrics"
)

// FilterRejectKind classifies filter errors into stable HTTP and metrics responses.
type FilterRejectKind uint8

// Filter rejection categories mapped to HTTP status and metric labels.
const (
	FilterRejectEmergencyBreaker FilterRejectKind = iota
	FilterRejectRateLimit
	FilterRejectDuplicate
	FilterRejectBudget
	FilterRejectPacing
	FilterRejectFreq
	FilterRejectGeo
	FilterRejectSchedule
	FilterRejectCampaignNotFound
	FilterRejectBidFloor
	FilterRejectTimeout
	FilterRejectFraud
	FilterRejectInfra
)

// FilterRejectSpec holds the HTTP response template for a rejection kind.
type FilterRejectSpec struct {
	Status      int
	Body        string
	GnetResp    []byte
	MetricLabel string
}

// FilterRejectSpecs is the lookup table from rejection kind to client response.
var FilterRejectSpecs = [...]FilterRejectSpec{
	FilterRejectEmergencyBreaker: {http.StatusServiceUnavailable, "service temporarily unavailable", RespEmergencyBreaker, "emergency_breaker"},
	FilterRejectRateLimit:        {http.StatusTooManyRequests, "rate limit exceeded", RespRateLimit, "rate_limit"},
	FilterRejectDuplicate:        {http.StatusConflict, "duplicate event", RespDuplicate, "duplicate"},
	FilterRejectBudget:           {http.StatusPaymentRequired, "budget exhausted", RespBudget, "budget"},
	FilterRejectPacing:           {http.StatusTooManyRequests, "pacing limit reached", RespPacing, "pacing"},
	FilterRejectFreq:             {http.StatusForbidden, "frequency limit reached", RespFreq, "freq"},
	FilterRejectGeo:              {http.StatusForbidden, "geo-targeting blocked", RespGeo, "geo"},
	FilterRejectSchedule:         {http.StatusForbidden, "outside delivery schedule", RespSchedule, "schedule"},
	FilterRejectCampaignNotFound: {http.StatusNotFound, "campaign not found", RespCampaignNotFound, "campaign_not_found"},
	FilterRejectBidFloor:         {http.StatusPaymentRequired, "bid floor not met", RespBidFloorNotMet, "bid_floor"},
	FilterRejectTimeout:          {http.StatusGatewayTimeout, "filter timeout", RespFilterTimeout, "filter_timeout"},
	FilterRejectFraud:            {http.StatusAccepted, "", nil, "fraud"},
	FilterRejectInfra:            {http.StatusServiceUnavailable, "service unavailable", RespInfraUnavailable, "infra_unavailable"},
}

// FraudReasonID indexes stable fraud signal codes shared by filters, metrics, and ClickHouse.
type FraudReasonID uint8

// Stable fraud_reason string values written to streams and fraud_events.
const (
	FraudReasonCodeDatacenterIP   = "datacenter_ip"
	FraudReasonCodeLowTTC         = "low_ttc"
	FraudReasonCodeMissingImpTS   = "missing_imp_ts"
	FraudReasonCodeL3Blocklist    = "l3_blocklist"
	FraudReasonCodeTLSBlocklist   = "tls_blocklist"
	FraudReasonCodeDeviceMismatch = "device_mismatch"
)

// Fraud signal identifiers; values are stable across deploys for metrics label binding.
const (
	FraudReasonNone FraudReasonID = iota
	FraudReasonDatacenterIP
	FraudReasonLowTTC
	FraudReasonMissingImpTS
	FraudReasonL3Blocklist
	FraudReasonTLSBlocklist
	FraudReasonDeviceMismatch
	fraudReasonCount
)

const (
	fraudSignalL1High uint8 = 1 << 0
	fraudSignalL2Weak uint8 = 1 << 1
	fraudSignalL3     uint8 = 1 << 2
)

type fraudReasonEntry struct {
	code   string
	weight uint8
	flags  uint8
}

// fraudReasonRegistry maps signal IDs to stable codes and weighted score contributions.
var fraudReasonRegistry = [fraudReasonCount]fraudReasonEntry{
	FraudReasonNone:           {},
	FraudReasonDatacenterIP:   {code: FraudReasonCodeDatacenterIP, weight: 45, flags: fraudSignalL1High},
	FraudReasonLowTTC:         {code: FraudReasonCodeLowTTC, weight: 45, flags: fraudSignalL1High},
	FraudReasonMissingImpTS:   {code: FraudReasonCodeMissingImpTS, weight: 35, flags: fraudSignalL2Weak},
	FraudReasonL3Blocklist:    {code: FraudReasonCodeL3Blocklist, weight: 100, flags: fraudSignalL3},
	FraudReasonTLSBlocklist:   {code: FraudReasonCodeTLSBlocklist, weight: 45, flags: fraudSignalL1High},
	FraudReasonDeviceMismatch: {code: FraudReasonCodeDeviceMismatch, weight: 35, flags: fraudSignalL2Weak},
}

// FraudReasonCode returns the stable string code for metrics and ClickHouse.
func FraudReasonCode(id FraudReasonID) string {
	if id >= fraudReasonCount {
		return ""
	}
	return fraudReasonRegistry[id].code
}

// FraudSignalWeight returns weighted score points for a registered fraud signal.
func FraudSignalWeight(id FraudReasonID) uint8 {
	if id >= fraudReasonCount {
		return 0
	}
	return fraudReasonRegistry[id].weight
}

// FraudSignalFlags returns L1/L2/L3 classification flags for a registered signal.
func FraudSignalFlags(id FraudReasonID) uint8 {
	if id >= fraudReasonCount {
		return 0
	}
	return fraudReasonRegistry[id].flags
}

// ClassifyFilterErr maps domain filter errors to a stable rejection kind.
func ClassifyFilterErr(err error) (FilterRejectKind, bool) {
	switch {
	case errors.Is(err, ErrEmergencyBreakerActive):
		return FilterRejectEmergencyBreaker, true
	case errors.Is(err, ErrRateLimitExceeded):
		return FilterRejectRateLimit, true
	case errors.Is(err, ErrDuplicateEvent):
		return FilterRejectDuplicate, true
	case errors.Is(err, ErrBudgetExhausted):
		return FilterRejectBudget, true
	case errors.Is(err, ErrPacingExhausted):
		return FilterRejectPacing, true
	case errors.Is(err, ErrFreqLimitExceeded):
		return FilterRejectFreq, true
	case errors.Is(err, ErrGeoBlocked):
		return FilterRejectGeo, true
	case errors.Is(err, ErrScheduleBlocked):
		return FilterRejectSchedule, true
	case errors.Is(err, ErrCampaignNotFound):
		return FilterRejectCampaignNotFound, true
	case errors.Is(err, ErrBidFloorNotMet):
		return FilterRejectBidFloor, true
	case errors.Is(err, context.DeadlineExceeded):
		return FilterRejectTimeout, true
	case errors.Is(err, ErrFraudDetected):
		return FilterRejectFraud, true
	case isInfraFilterErr(err):
		return FilterRejectInfra, true
	default:
		return 0, false
	}
}

// isInfraFilterErr treats Redis circuit and network faults as retryable infra failures.
func isInfraFilterErr(err error) bool {
	if errors.Is(err, database.ErrRedisCircuitOpen) {
		return true
	}
	return database.IsNetworkOrSystemError(err)
}

// RecordHTTPFilterReject increments stdlib HTTP track blocked counters.
func RecordHTTPFilterReject(kind FilterRejectKind) {
	metrics.FilterBlockedTotal.WithLabelValues(FilterRejectSpecs[kind].MetricLabel).Inc()
}
