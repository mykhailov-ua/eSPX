package filter

// Prebuilt gnet HTTP response bytes avoid fmt and strconv on the track rejection hot path.
var (
	RespInvalidProto       = []byte("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 16\r\nConnection: keep-alive\r\n\r\ninvalid protobuf")
	RespInvalidCampaign    = []byte("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 19\r\nConnection: keep-alive\r\n\r\ninvalid campaign_id")
	RespInvalidJSON        = []byte("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 12\r\nConnection: keep-alive\r\n\r\ninvalid json")
	RespEmergencyBreaker   = []byte("HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nContent-Length: 32\r\nConnection: keep-alive\r\n\r\nservice temporarily unavailable")
	RespWorkerPoolOverload = []byte("HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nRetry-After: 1\r\nContent-Length: 17\r\nConnection: keep-alive\r\n\r\nserver overloaded")
	RespInfraUnavailable   = []byte("HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nRetry-After: 1\r\nContent-Length: 19\r\nConnection: keep-alive\r\n\r\nservice unavailable")
	RespRateLimit          = []byte("HTTP/1.1 429 Too Many Requests\r\nContent-Type: text/plain\r\nRetry-After: 60\r\nContent-Length: 19\r\nConnection: keep-alive\r\n\r\nrate limit exceeded")
	RespDuplicate          = []byte("HTTP/1.1 409 Conflict\r\nContent-Type: text/plain\r\nContent-Length: 15\r\nConnection: keep-alive\r\n\r\nduplicate event")
	RespBudget             = []byte("HTTP/1.1 402 Payment Required\r\nContent-Type: text/plain\r\nContent-Length: 16\r\nConnection: keep-alive\r\n\r\nbudget exhausted")
	RespPacing             = []byte("HTTP/1.1 429 Too Many Requests\r\nContent-Type: text/plain\r\nRetry-After: 60\r\nContent-Length: 20\r\nConnection: keep-alive\r\n\r\npacing limit reached")
	RespFreq               = []byte("HTTP/1.1 403 Forbidden\r\nContent-Type: text/plain\r\nContent-Length: 23\r\nConnection: keep-alive\r\n\r\nfrequency limit reached")
	RespGeo                = []byte("HTTP/1.1 403 Forbidden\r\nContent-Type: text/plain\r\nContent-Length: 21\r\nConnection: keep-alive\r\n\r\ngeo-targeting blocked")
	RespSchedule           = []byte("HTTP/1.1 403 Forbidden\r\nContent-Type: text/plain\r\nContent-Length: 26\r\nConnection: keep-alive\r\n\r\noutside delivery schedule")
	RespCampaignNotFound   = []byte("HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: 17\r\nConnection: keep-alive\r\n\r\ncampaign not found")
	RespBidFloorNotMet     = []byte("HTTP/1.1 402 Payment Required\r\nContent-Type: text/plain\r\nContent-Length: 17\r\nConnection: keep-alive\r\n\r\nbid floor not met")
	RespFilterTimeout      = []byte("HTTP/1.1 504 Gateway Timeout\r\nContent-Type: text/plain\r\nContent-Length: 15\r\nConnection: keep-alive\r\n\r\nfilter timeout")
	RespInternalError      = []byte("HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: keep-alive\r\n\r\ninternal error")
	RespBadRequestClose    = []byte("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
	RespNotFound           = []byte("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
	RespMethodNotAllowed   = []byte("HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n")
	RespPayloadTooLarge    = []byte("HTTP/1.1 413 Payload Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
)

// Legacy unexported aliases for filter/errors.go.
var (
	respEmergencyBreaker = RespEmergencyBreaker
	respRateLimit        = RespRateLimit
	respDuplicate        = RespDuplicate
	respBudget           = RespBudget
	respPacing           = RespPacing
	respFreq             = RespFreq
	respGeo              = RespGeo
	respSchedule         = RespSchedule
	respCampaignNotFound = RespCampaignNotFound
	respBidFloorNotMet   = RespBidFloorNotMet
	respFilterTimeout    = RespFilterTimeout
	respInfraUnavailable = RespInfraUnavailable
	respFraud            = []byte("HTTP/1.1 403 Forbidden\r\nContent-Type: text/plain\r\nContent-Length: 14\r\nConnection: keep-alive\r\n\r\nfraud detected")
)
