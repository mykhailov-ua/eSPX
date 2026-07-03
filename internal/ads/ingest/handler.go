package ingest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/ads/clock"
	"espx/internal/ads/filter"
	"espx/internal/ads/pb"
	"espx/internal/ads/processor"
	"espx/internal/ads/repo"
	"espx/internal/ads/sharding"
	"espx/internal/config"
	"espx/internal/domain"
	"espx/internal/metrics"
	"espx/pkg/logger"

	"github.com/google/uuid"
	"github.com/panjf2000/gnet/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// adEventPool recycles protobuf track requests on the HTTP and gnet paths.
var (
	adEventPool = sync.Pool{
		New: func() any {
			return &pb.AdEvent{
				Metadata: &pb.EventMetadata{},
			}
		},
	}
	trackResponsePool = sync.Pool{
		New: func() any { return &pb.TrackResponse{} },
	}
	trackRequestPool = sync.Pool{
		New: func() any {
			return &TrackRequest{}
		},
	}
	bufferPool = sync.Pool{
		New: func() any { return new(bytes.Buffer) },
	}
	fraudValuesPool = sync.Pool{
		New: func() any {
			s := make([]any, 22)
			return &s
		},
	}
	responseBytesPool = sync.Pool{
		New: func() any {
			s := make([]byte, 4096)
			return &s
		},
	}
	extraBufPool = sync.Pool{
		New: func() any {
			s := make([]byte, 0, 1024)
			return &s
		},
	}
	statusStrings          [600]string
	maxPoolObjectSize      = 64 * 1024
	contentTypeProtoHeader = []string{"application/x-protobuf"}
	contentTypeJsonHeader  = []string{"application/json"}
)

// connContext holds per-connection parse and response buffers for the gnet handler.
type connContext struct {
	pbReq    pb.AdEvent
	trackReq TrackRequest
	evt      domain.Event
	valSlice []any
	resp     pb.TrackResponse
	bufSlice []byte
	extraBuf []byte
	wReqID   filter.BufWrapper
	wCamp    filter.BufWrapper
	wTime    filter.BufWrapper
	remoteIP string
	shardID  int
}

// init materializes HTTP status label strings once so gnet track metrics avoid per-request strconv.
func init() {
	for i := 0; i < 600; i++ {
		statusStrings[i] = strconv.Itoa(i)
	}
}

// putBuffer recycles response buffers only when capacity stays bounded for the gnet track path.
func putBuffer(buf *bytes.Buffer) {
	if buf == nil || buf.Cap() > maxPoolObjectSize {
		return
	}
	buf.Reset()
	bufferPool.Put(buf)
}

// putAdEvent resets and pools a protobuf AdEvent, dropping oversized metadata maps.
func putAdEvent(evt *pb.AdEvent) {
	if evt == nil {
		return
	}
	if evt.Metadata != nil && (len(evt.Metadata.ExtraKeys) > 100 || cap(evt.Metadata.ExtraBytes) > 4096) {
		evt.Reset()
		adEventPool.Put(evt)
		return
	}
	evt.CampaignId = evt.CampaignId[:0]
	evt.EventType = evt.EventType[:0]
	if evt.Metadata != nil {
		evt.Metadata.ClickId = evt.Metadata.ClickId[:0]
		evt.Metadata.UserId = evt.Metadata.UserId[:0]
		evt.Metadata.DeviceType = evt.Metadata.DeviceType[:0]
		evt.Metadata.Os = evt.Metadata.Os[:0]
		for i := range evt.Metadata.ExtraKeys {
			evt.Metadata.ExtraKeys[i] = evt.Metadata.ExtraKeys[i][:0]
		}
		evt.Metadata.ExtraKeys = evt.Metadata.ExtraKeys[:0]
		for i := range evt.Metadata.ExtraValues {
			evt.Metadata.ExtraValues[i] = evt.Metadata.ExtraValues[i][:0]
		}
		evt.Metadata.ExtraValues = evt.Metadata.ExtraValues[:0]
		evt.Metadata.ExtraBytes = evt.Metadata.ExtraBytes[:0]
	}
	adEventPool.Put(evt)
}

// putTrackResponse resets and pools a protobuf TrackResponse.
func putTrackResponse(resp *pb.TrackResponse) {
	if resp == nil {
		return
	}
	resp.Reset()
	trackResponsePool.Put(resp)
}

// Pinger supports health checks against Postgres or other backing stores.
type Pinger interface {
	Ping(ctx context.Context) error
}

// NewRouter builds the stdlib HTTP track handler with health, metrics, and pprof routes.
// Deprecated: production ingestion uses gnet (AdsPacketHandler). POST /track delegates to
// shared processTrack(); this router remains for integration and fault tests only.
func NewRouter(cfg *config.Config, registry domain.CampaignRegistry, filterEngine *filter.FilterEngine, pool Pinger, rdbs []redis.UniversalClient, sharder sharding.Sharder, fraudStream string, creativeStore *filter.BrandCreativeStore) http.Handler {
	mux := http.NewServeMux()

	trackDurationObserver := metrics.HttpRequestDuration.WithLabelValues("POST", "/track")
	var trackStatusCounters [600]prometheus.Counter
	for i := 0; i < 600; i++ {
		trackStatusCounters[i] = metrics.HttpRequestsTotal.WithLabelValues("POST", "/track", statusStrings[i])
	}

	trackLatencyRing := NewLatencyRing(defaultLatencyRingCap)
	fraudWriter := processor.NewFraudStreamWriter(rdbs, fraudStream, int64(cfg.StreamMaxLen))
	trackProc := newTrackProcessor(filterEngine, registry, creativeStore)

	mux.Handle("GET /metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trackLatencyRing.FlushTo(trackDurationObserver)
		promhttp.Handler().ServeHTTP(w, r)
	}))

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := pool.Ping(ctx); err != nil {
			slog.Error("health check failed: postgres", "error", err)
			http.Error(w, "postgres unreachable", http.StatusServiceUnavailable)
			return
		}

		for i, rdb := range rdbs {
			if err := rdb.Ping(ctx).Err(); err != nil {
				slog.Error("health check failed: redis shard", "shard", i, "error", err)
				http.Error(w, "redis shard unreachable", http.StatusServiceUnavailable)
				return
			}
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	mux.HandleFunc("POST /track", func(w http.ResponseWriter, r *http.Request) {
		startMono := filter.MonotonicNano()
		status := http.StatusAccepted

		defer func() {
			if status >= 0 && status < 600 {
				trackStatusCounters[status].Inc()
			} else {
				metrics.HttpRequestsTotal.WithLabelValues("POST", "/track", strconv.Itoa(status)).Inc()
			}
			trackLatencyRing.RecordMono(startMono)
		}()

		if r.ContentLength > cfg.MaxRequestBodySize {
			metrics.HttpParseErrors.WithLabelValues("payload_too_large").Inc()
			status = http.StatusBadRequest
			http.Error(w, "invalid body", status)
			return
		}
		if r.ContentLength < 0 {
			r.Body = http.MaxBytesReader(w, r.Body, cfg.MaxRequestBodySize)
		}

		id, _ := clock.NewFastUUID()
		wReqID := filter.BufPool.Get().(*filter.BufWrapper)
		wReqID.Buf = wReqID.Buf[:0]
		wReqID.Buf = repo.AppendUUID(wReqID.Buf, id)
		defer filter.BufPool.Put(wReqID)

		var campaignID uuid.UUID
		var eventType string
		var userID string
		var payload []byte

		ip := extractClientIP(r, cfg.TrustedProxies)
		var clickID string
		var requestIDStr string

		contentType := ""
		if ctSlice := r.Header["Content-Type"]; len(ctSlice) > 0 {
			contentType = ctSlice[0]
		}
		if contentType == "application/x-protobuf" || contentType == "" {
			buf := bufferPool.Get().(*bytes.Buffer)
			defer putBuffer(buf)

			if _, err := io.Copy(buf, r.Body); err != nil {
				metrics.HttpParseErrors.WithLabelValues("read_body").Inc()
				status = http.StatusBadRequest
				http.Error(w, "invalid body", status)
				return
			}

			pbReq := adEventPool.Get().(*pb.AdEvent)
			defer putAdEvent(pbReq)

			if err := pbReq.UnmarshalVT(buf.Bytes()); err != nil {
				metrics.HttpParseErrors.WithLabelValues("invalid_proto").Inc()
				status = http.StatusBadRequest
				http.Error(w, "invalid protobuf", status)
				return
			}

			var cid uuid.UUID
			if !ParseUUID(pbReq.CampaignId, &cid) {
				metrics.HttpParseErrors.WithLabelValues("invalid_campaign_id").Inc()
				status = http.StatusBadRequest
				http.Error(w, "invalid campaign_id", status)
				return
			}
			campaignID = cid
			eventType = repo.UnsafeString(pbReq.EventType)
			if pbReq.Metadata != nil {
				userID = repo.UnsafeString(pbReq.Metadata.UserId)
				if len(pbReq.Metadata.ClickId) > 0 {
					clickID = repo.UnsafeString(pbReq.Metadata.ClickId)
				}
				if len(pbReq.Metadata.ExtraBytes) > 0 {
					payload = pbReq.Metadata.ExtraBytes
				} else if len(pbReq.Metadata.ExtraKeys) > 0 {
					bufPtr := extraBufPool.Get().(*[]byte)
					*bufPtr = marshalExtra(*bufPtr, pbReq.Metadata.ExtraKeys, pbReq.Metadata.ExtraValues)
					payload = *bufPtr
					defer extraBufPool.Put(bufPtr)
				}
			}
		} else {
			buf := bufferPool.Get().(*bytes.Buffer)
			defer putBuffer(buf)

			if _, err := io.Copy(buf, r.Body); err != nil {
				metrics.HttpParseErrors.WithLabelValues("read_body").Inc()
				status = http.StatusBadRequest
				http.Error(w, "invalid body", status)
				return
			}

			req := trackRequestPool.Get().(*TrackRequest)
			req.Reset()
			defer trackRequestPool.Put(req)

			err := ParseTrackRequestJSON(req, buf.Bytes())
			if err != nil {
				metrics.HttpParseErrors.WithLabelValues("invalid_json").Inc()
				status = http.StatusBadRequest
				http.Error(w, "invalid json", status)
				return
			}
			campaignID = req.CampaignID
			userID = req.UserID
			eventType = req.Type
			payload = req.Payload
			if req.ClickID != "" {
				clickID = req.ClickID
			}
		}

		if clickID == "" {
			requestIDStr = repo.UnsafeString(wReqID.Buf)
			clickID = requestIDStr
		}

		evt := domain.EventPool.Get().(*domain.Event)
		evt.Reset()
		evt.ClickID = clickID
		evt.CampaignID = campaignID
		evt.UserID = userID
		evt.Type = eventType
		evt.Payload = append(evt.Payload[:0], payload...)
		evt.IP = ip
		ua := ""
		if uaSlice := r.Header["User-Agent"]; len(uaSlice) > 0 {
			ua = uaSlice[0]
		}
		evt.UA = ua

		var landing string
		if filterEngine != nil {
			outcome := processTrack(trackProc, evt, nil)
			switch outcome.Status {
			case trackStatusFraudAccepted:
				filter.RecordHTTPFilterReject(outcome.RejectKind)
				shard := sharder.GetShard(evt.CampaignID)
				processor.EnqueueFraudReject(fraudWriter, shard, evt)
				domain.EventPool.Put(evt)
				accept := ""
				if accSlice := r.Header["Accept"]; len(accSlice) > 0 {
					accept = accSlice[0]
				}
				writeHTTPTrackAccepted(w, wReqID, requestIDStr, accept, "")
				return
			case trackStatusRejected:
				spec := filter.FilterRejectSpecs[outcome.RejectKind]
				domain.EventPool.Put(evt)
				filter.RecordHTTPFilterReject(outcome.RejectKind)
				if outcome.RejectKind == filter.FilterRejectInfra {
					w.Header().Set("Retry-After", "1")
				}
				if outcome.RejectKind == filter.FilterRejectRateLimit || outcome.RejectKind == filter.FilterRejectPacing {
					w.Header().Set("Retry-After", "60")
				}
				http.Error(w, spec.Body, spec.Status)
				return
			case trackStatusInternalError:
				domain.EventPool.Put(evt)
				http.Error(w, "internal error", http.StatusInternalServerError)
				return
			case trackStatusAccepted:
				landing = outcome.LandingURL
			}
		} else {
			landing = filter.ResolveLandingURL(registry, creativeStore, evt)
		}
		domain.EventPool.Put(evt)

		accept := ""
		if accSlice := r.Header["Accept"]; len(accSlice) > 0 {
			accept = accSlice[0]
		}
		writeHTTPTrackAccepted(w, wReqID, requestIDStr, accept, landing)
	})

	return mux
}

// isTrustedProxy reports whether a remote address may supply forwarded client IPs.
func isTrustedProxy(ipStr string, trustedProxies []string) bool {
	if len(trustedProxies) == 0 {
		return false
	}
	parsedIP := net.ParseIP(ipStr)
	if parsedIP == nil {
		return false
	}
	for _, p := range trustedProxies {
		if p == "" {
			continue
		}
		if p == ipStr {
			return true
		}
		if _, ipNet, err := net.ParseCIDR(p); err == nil {
			if ipNet.Contains(parsedIP) {
				return true
			}
		}
	}
	return false
}

// getIPOnly strips the port from a host:port remote address string.
func getIPOnly(addr string) string {
	if idx := strings.LastIndexByte(addr, ':'); idx != -1 {
		if idx > 0 && addr[idx-1] == ']' {
			if addr[0] == '[' {
				return addr[1 : idx-1]
			}
		}
		return addr[:idx]
	}
	return addr
}

// extractClientIP resolves the client IP from trusted proxy headers or RemoteAddr.
func extractClientIP(r *http.Request, trustedProxies []string) string {
	remoteIP := getIPOnly(r.RemoteAddr)
	if !isTrustedProxy(remoteIP, trustedProxies) {
		return remoteIP
	}

	var xff string
	if xffSlice := r.Header["X-Forwarded-For"]; len(xffSlice) > 0 {
		xff = xffSlice[0]
	}
	if xff != "" {
		last := len(xff)
		for i := len(xff) - 1; i >= -1; i-- {
			if i == -1 || xff[i] == ',' {
				start := i + 1
				for start < last && xff[start] == ' ' {
					start++
				}
				end := last
				for end > start && xff[end-1] == ' ' {
					end--
				}

				if start < end {
					ipStr := xff[start:end]
					parsedIP := net.ParseIP(ipStr)
					if parsedIP != nil && !parsedIP.IsPrivate() && !parsedIP.IsLoopback() && !parsedIP.IsLinkLocalUnicast() {
						return ipStr
					}
				}
				last = i
			}
		}
	}

	if xriSlice := r.Header["X-Real-Ip"]; len(xriSlice) > 0 {
		xri := xriSlice[0]
		ipStr := strings.TrimSpace(xri)
		parsedIP := net.ParseIP(ipStr)
		if parsedIP != nil && !parsedIP.IsPrivate() && !parsedIP.IsLoopback() && !parsedIP.IsLinkLocalUnicast() {
			return ipStr
		}
	}

	return remoteIP
}

// AdsPacketHandler serves /track over gnet with optional worker-pool offload.
type AdsPacketHandler struct {
	*gnet.BuiltinEventEngine
	eng                   *gnet.Engine
	filterEngine          *filter.FilterEngine
	registry              domain.CampaignRegistry
	creativeStore         *filter.BrandCreativeStore
	cfg                   *config.Config
	pool                  Pinger
	rdbs                  []redis.UniversalClient
	sharder               sharding.Sharder
	fraudStream           string
	trackDurationObserver prometheus.Observer
	trackStatusCounters   [600]prometheus.Counter
	trackMetrics          preboundTrackMetrics
	trackLatencyRing      *LatencyRing
	healthy               atomic.Int32
	rdbsHealthy           []atomic.Int32
	logger                *logger.Logger
	loggerShardCounter    atomic.Uint64
	auditLogSeq           atomic.Uint64
	auditLogSampleMask    uint64
	fraudWriter           *processor.FraudStreamWriter
	trackProc             trackProcessor
	contextPool           sync.Pool
	workerPool            *PinnedWorkerPool
}

// SetLogger attaches the audit log writer for accepted gnet track events.
func (h *AdsPacketHandler) SetLogger(l *logger.Logger) {
	h.logger = l
}

// SetWorkerPool enables pinned-thread offload for React work.
func (h *AdsPacketHandler) SetWorkerPool(wp *PinnedWorkerPool) {
	h.workerPool = wp
}

// write sends a response and returns connContext to the pool when using a worker pool.
func (h *AdsPacketHandler) write(c gnet.Conn, data []byte, ctx *connContext) {
	if h.workerPool != nil && ctx != nil {
		_ = c.AsyncWrite(data, func(c gnet.Conn, err error) error {
			h.contextPool.Put(ctx)
			return nil
		})
	} else {
		_, _ = c.Write(data)
	}
}

// NewAdsPacketHandler constructs the gnet track server with pre-bound metrics.
func NewAdsPacketHandler(cfg *config.Config, registry domain.CampaignRegistry, filterEngine *filter.FilterEngine, pool Pinger, rdbs []redis.UniversalClient, sharder sharding.Sharder, fraudStream string, creativeStore *filter.BrandCreativeStore) *AdsPacketHandler {
	trackDurationObserver := metrics.HttpRequestDuration.WithLabelValues("POST", "/track")
	var trackStatusCounters [600]prometheus.Counter
	for i := 0; i < 600; i++ {
		trackStatusCounters[i] = metrics.HttpRequestsTotal.WithLabelValues("POST", "/track", statusStrings[i])
	}

	h := &AdsPacketHandler{
		filterEngine:          filterEngine,
		registry:              registry,
		creativeStore:         creativeStore,
		cfg:                   cfg,
		pool:                  pool,
		rdbs:                  rdbs,
		sharder:               sharder,
		fraudStream:           fraudStream,
		fraudWriter:           processor.NewFraudStreamWriter(rdbs, fraudStream, int64(cfg.StreamMaxLen)),
		trackProc:             newTrackProcessor(filterEngine, registry, creativeStore),
		trackDurationObserver: trackDurationObserver,
		trackStatusCounters:   trackStatusCounters,
		trackMetrics:          newPreboundTrackMetrics(),
		trackLatencyRing:      NewLatencyRing(defaultLatencyRingCap),
		auditLogSampleMask:    processor.AuditLogSampleMaskFromConfig(cfg.AuditLogSampleMask),
	}
	if n := len(rdbs); n > 0 {
		h.rdbsHealthy = make([]atomic.Int32, n)
		for i := range h.rdbsHealthy {
			h.rdbsHealthy[i].Store(1)
		}
	}

	h.contextPool = sync.Pool{
		New: func() any {
			return &connContext{
				pbReq: pb.AdEvent{
					Metadata: &pb.EventMetadata{},
				},
				trackReq: TrackRequest{
					Payload: make([]byte, 0, 512),
				},
				evt: domain.Event{
					Payload: make([]byte, 0, 1024),
				},
				valSlice: make([]any, 18),
				resp:     pb.TrackResponse{},
				bufSlice: make([]byte, 4096),
				wReqID: filter.BufWrapper{
					Buf: make([]byte, 0, 128),
				},
				wCamp: filter.BufWrapper{
					Buf: make([]byte, 0, 128),
				},
				wTime: filter.BufWrapper{
					Buf: make([]byte, 0, 128),
				},
			}
		},
	}

	return h
}

// recordMetrics updates pre-bound counters and the latency ring for one gnet request.
func (h *AdsPacketHandler) recordMetrics(startMono int64, status int) {
	if status >= 0 && status < 600 {
		h.trackStatusCounters[status].Inc()
	} else {
		metrics.HttpRequestsTotal.WithLabelValues("POST", "/track", strconv.Itoa(status)).Inc()
	}
	h.trackLatencyRing.RecordMono(startMono)
}

// FlushLatency exports buffered latency samples during metrics scrape only.
func (h *AdsPacketHandler) FlushLatency() {
	if h.trackLatencyRing != nil {
		h.trackLatencyRing.FlushTo(h.trackDurationObserver)
	}
}

// SetHealthProbeState mirrors StartHealthProbe atomics for gnet health tests.
func (h *AdsPacketHandler) SetHealthProbeState(healthy bool, shardOK ...bool) {
	if healthy {
		h.healthy.Store(1)
	} else {
		h.healthy.Store(0)
	}
	for i, ok := range shardOK {
		if i >= len(h.rdbsHealthy) {
			break
		}
		if ok {
			h.rdbsHealthy[i].Store(1)
		} else {
			h.rdbsHealthy[i].Store(0)
		}
	}
}

// writeGnetTrackAccepted emits a 202 track response over a gnet connection.
func (h *AdsPacketHandler) writeGnetTrackAccepted(ctx *connContext, req parsedHTTPRequest, c gnet.Conn, startMono int64, wReqID *filter.BufWrapper, requestIDStr, landingURL string) {
	if requestIDStr == "" {
		requestIDStr = repo.UnsafeString(wReqID.Buf)
	}

	accept := repo.UnsafeString(req.Accept)
	if accept == "application/x-protobuf" {
		resp := &ctx.resp
		resp.Reset()
		resp.RequestId = requestIDStr
		resp.Status = "accepted"

		respSize := resp.SizeVT()
		bufSlice := ctx.bufSlice
		if cap(bufSlice) < 200+respSize {
			bufSlice = make([]byte, 200+respSize)
			ctx.bufSlice = bufSlice
		} else {
			bufSlice = bufSlice[:200+respSize]
		}

		offset := copy(bufSlice, "HTTP/1.1 202 Accepted\r\nContent-Type: application/x-protobuf\r\nContent-Length: ")
		offset += copy(bufSlice[offset:], strconv.Itoa(respSize))
		offset += copy(bufSlice[offset:], "\r\nConnection: keep-alive\r\n\r\n")

		n, err := resp.MarshalToVT(bufSlice[offset : offset+respSize])
		if err != nil {
			h.write(c, filter.RespInternalError, ctx)
			h.recordMetrics(startMono, http.StatusInternalServerError)
			return
		}
		outSlice := bufSlice[:offset+n]
		metrics.GnetBytesSent.Add(float64(len(outSlice)))
		metrics.GnetPacketsSent.Inc()
		h.write(c, outSlice, ctx)
	} else {
		reqID := wReqID.Buf
		if requestIDStr != "" {
			reqID = repo.UnsafeBytes(requestIDStr)
		}

		const jsonPrefix = `{"request_id":"`
		const jsonMid = `","status":"accepted"`
		respSize := len(jsonPrefix) + len(reqID) + len(jsonMid) + 1
		if landingURL != "" {
			const jsonLand = `,"landing_url":"`
			respSize += len(jsonLand) + len(landingURL) + 1
		}

		bufSlice := ctx.bufSlice
		if cap(bufSlice) < 200+respSize {
			bufSlice = make([]byte, 200+respSize)
			ctx.bufSlice = bufSlice
		} else {
			bufSlice = bufSlice[:200+respSize]
		}

		offset := copy(bufSlice, "HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: ")
		offset += copy(bufSlice[offset:], strconv.Itoa(respSize))
		offset += copy(bufSlice[offset:], "\r\nConnection: keep-alive\r\n\r\n")
		offset += copy(bufSlice[offset:], jsonPrefix)
		offset += copy(bufSlice[offset:], reqID)
		offset += copy(bufSlice[offset:], jsonMid)
		if landingURL != "" {
			offset += copy(bufSlice[offset:], `,"landing_url":"`)
			offset += copy(bufSlice[offset:], landingURL)
			bufSlice[offset] = '"'
			offset++
		}
		bufSlice[offset] = '}'
		offset++

		metrics.GnetBytesSent.Add(float64(offset))
		metrics.GnetPacketsSent.Inc()
		h.write(c, bufSlice[:offset], ctx)
	}

	h.recordMetrics(startMono, http.StatusAccepted)
}

// writeHTTPTrackAccepted emits a 202 track response on the stdlib HTTP handler.
func writeHTTPTrackAccepted(w http.ResponseWriter, wReqID *filter.BufWrapper, requestIDStr string, accept string, landingURL string) {
	if requestIDStr == "" {
		requestIDStr = repo.UnsafeString(wReqID.Buf)
	}
	if accept == "application/x-protobuf" {
		resp := trackResponsePool.Get().(*pb.TrackResponse)
		defer putTrackResponse(resp)
		resp.RequestId = requestIDStr
		resp.Status = "accepted"

		respSize := resp.SizeVT()
		bufSlicePtr := responseBytesPool.Get().(*[]byte)
		bufSlice := *bufSlicePtr
		if cap(bufSlice) < respSize {
			bufSlice = make([]byte, respSize)
		} else {
			bufSlice = bufSlice[:respSize]
		}

		n, err := resp.MarshalToVT(bufSlice)
		if err != nil {
			*bufSlicePtr = bufSlice
			responseBytesPool.Put(bufSlicePtr)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		out := bufSlice[:n]
		w.Header()["Content-Type"] = contentTypeProtoHeader
		w.WriteHeader(http.StatusAccepted)
		w.Write(out)
		*bufSlicePtr = bufSlice
		responseBytesPool.Put(bufSlicePtr)
		return
	}

	w.Header()["Content-Type"] = contentTypeJsonHeader
	w.WriteHeader(http.StatusAccepted)
	buf := bufferPool.Get().(*bytes.Buffer)
	defer putBuffer(buf)
	buf.WriteString(`{"request_id":"`)
	buf.Write(wReqID.Buf)
	buf.WriteString(`","status":"accepted"`)
	if landingURL != "" {
		buf.WriteString(`,"landing_url":"`)
		buf.WriteString(landingURL)
		buf.WriteByte('"')
	}
	buf.WriteByte('}')
	w.Write(buf.Bytes())
}

// OnBoot stores the gnet engine handle for graceful shutdown.
func (h *AdsPacketHandler) OnBoot(eng gnet.Engine) (action gnet.Action) {
	slog.Info("gnet server is booting")
	h.eng = &eng
	return gnet.None
}

// Stop shuts down fraud streaming and the gnet engine.
func (h *AdsPacketHandler) Stop(ctx context.Context) error {
	if h.fraudWriter != nil {
		h.fraudWriter.Stop()
	}
	if h.eng != nil {
		return h.eng.Stop(ctx)
	}
	return nil
}

// StartHealthProbe periodically pings Postgres and Redis for the gnet health endpoint.
func (h *AdsPacketHandler) StartHealthProbe(ctx context.Context) {
	h.healthy.Store(1)
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				ok := true
				if h.pool != nil {
					if err := h.pool.Ping(probeCtx); err != nil {
						ok = false
						slog.Error("health probe: postgres unreachable", "error", err)
					}
				}
				for i, rdb := range h.rdbs {
					if err := rdb.Ping(probeCtx).Err(); err != nil {
						ok = false
						if i < len(h.rdbsHealthy) {
							h.rdbsHealthy[i].Store(0)
						}
						slog.Error("health probe: redis shard unreachable", "shard", i, "error", err)
					} else if i < len(h.rdbsHealthy) {
						h.rdbsHealthy[i].Store(1)
					}
				}
				cancel()
				if ok {
					h.healthy.Store(1)
				} else {
					h.healthy.Store(0)
				}
				shardStates := make([]int32, len(h.rdbsHealthy))
				for i := range h.rdbsHealthy {
					shardStates[i] = h.rdbsHealthy[i].Load()
				}
				exportHealthProbeMetrics(ok, shardStates)
			}
		}
	}()
}

// OnOpen tracks active gnet connections for capacity metrics.
func (h *AdsPacketHandler) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	metrics.GnetActiveConnections.Inc()
	return nil, gnet.None
}

// OnClose decrements active gnet connection metrics.
func (h *AdsPacketHandler) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	metrics.GnetActiveConnections.Dec()
	return gnet.None
}

// requestBufferPool recycles copied request buffers for worker-pool React offload.
var requestBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 4096)
		return &b
	},
}

// OnTraffic parses inbound HTTP requests and dispatches them synchronously or via the worker pool.
func (h *AdsPacketHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {

	loopStart := filter.MonotonicNano()
	defer func() {
		metrics.GnetEventLoopWorkDuration.Add(filter.MonoElapsedSeconds(loopStart))
	}()

	for {
		inboundBuffered := c.InboundBuffered()
		if inboundBuffered == 0 {
			break
		}
		buf, err := c.Peek(inboundBuffered)
		if err != nil {
			return gnet.Close
		}

		metrics.GnetBytesReceived.Add(float64(len(buf)))
		metrics.GnetPacketsReceived.Inc()

		reqLen, req, err := h.parseHTTP(buf)
		if err != nil {
			if errors.Is(err, errIncompleteRequest) {
				metrics.HttpParseErrors.WithLabelValues("incomplete").Inc()
				break
			}
			if errors.Is(err, errPayloadTooLarge) {
				metrics.HttpParseErrors.WithLabelValues("payload_too_large").Inc()
				_, _ = c.Write(filter.RespPayloadTooLarge)
				return gnet.Close
			}
			metrics.HttpParseErrors.WithLabelValues("invalid").Inc()
			_, _ = c.Write(filter.RespBadRequestClose)
			return gnet.Close
		}

		if h.workerPool != nil {
			reqBufPtr := requestBufferPool.Get().(*[]byte)
			reqBytes := *reqBufPtr
			if cap(reqBytes) < reqLen {
				reqBytes = make([]byte, reqLen)
			} else {
				reqBytes = reqBytes[:reqLen]
			}
			copy(reqBytes, buf[:reqLen])

			if _, err := c.Discard(reqLen); err != nil {
				requestBufferPool.Put(reqBufPtr)
				return gnet.Close
			}

			ctx := h.contextPool.Get().(*connContext)
			if h.logger != nil {
				ctx.shardID = int(h.loggerShardCounter.Add(1) % uint64(len(h.logger.Shards())))
			}

			submitted := h.workerPool.Submit(func() {
				defer requestBufferPool.Put(reqBufPtr)
				c.SetContext(ctx)
				_, reqParsed, err := h.parseHTTP(reqBytes)
				if err != nil {
					h.write(c, filter.RespBadRequestClose, ctx)
					return
				}
				_ = h.React(reqParsed, c)
			})
			if !submitted {
				requestBufferPool.Put(reqBufPtr)
				h.contextPool.Put(ctx)
				metrics.WorkerPoolRejectTotal.Inc()
				h.write(c, filter.RespWorkerPoolOverload, nil)
			}
		} else {
			act := h.React(req, c)
			if _, err := c.Discard(reqLen); err != nil {
				return gnet.Close
			}

			if act != gnet.None {
				return act
			}
		}
	}
	return gnet.None
}

// parsedHTTPRequest holds zero-copy views into the gnet inbound buffer for one request.
type parsedHTTPRequest struct {
	Method           []byte
	Path             []byte
	ContentType      []byte
	ClientIP         []byte
	UserAgent        []byte
	Accept           []byte
	TLSHash          []byte
	SecCHUA          []byte
	AcceptLang       []byte
	Body             []byte
	ContentLength    int
	HasContentLength bool
}

// HTTP parse errors for incomplete, invalid, and oversized gnet requests.
var (
	errIncompleteRequest = errors.New("incomplete HTTP request")
	errInvalidRequest    = errors.New("invalid HTTP request")
	errPayloadTooLarge   = errors.New("payload too large")
)

// parseHTTP extracts one HTTP request from the gnet inbound buffer without copying the body.
func (h *AdsPacketHandler) parseHTTP(data []byte) (int, parsedHTTPRequest, error) {
	var req parsedHTTPRequest

	lineEnd := bytes.Index(data, []byte("\r\n"))
	if lineEnd < 0 {
		return 0, req, errIncompleteRequest
	}
	reqLine := data[:lineEnd]

	space1 := bytes.IndexByte(reqLine, ' ')
	if space1 < 0 {
		return 0, req, errInvalidRequest
	}
	req.Method = reqLine[:space1]

	rest := reqLine[space1+1:]
	space2 := bytes.IndexByte(rest, ' ')
	if space2 < 0 {
		return 0, req, errInvalidRequest
	}
	req.Path = rest[:space2]

	idx := lineEnd + 2
	for {
		if idx >= len(data) {
			return 0, req, errIncompleteRequest
		}
		if idx+2 <= len(data) && data[idx] == '\r' && data[idx+1] == '\n' {
			idx += 2
			break
		}

		lineEnd = bytes.Index(data[idx:], []byte("\r\n"))
		if lineEnd < 0 {
			return 0, req, errIncompleteRequest
		}
		headerLine := data[idx : idx+lineEnd]
		idx += lineEnd + 2

		colonIdx := bytes.IndexByte(headerLine, ':')
		if colonIdx < 0 {
			continue
		}

		key := trimSpaceBytes(headerLine[:colonIdx])
		val := trimSpaceBytes(headerLine[colonIdx+1:])

		if equalFoldBytes(key, []byte("content-length")) {
			req.ContentLength = parseDecimal(val)
			req.HasContentLength = true
		} else if equalFoldBytes(key, []byte("content-type")) {
			req.ContentType = val
		} else if equalFoldBytes(key, []byte("x-forwarded-for")) {
			req.ClientIP = val
		} else if equalFoldBytes(key, []byte("x-real-ip")) {
			if len(req.ClientIP) == 0 {
				req.ClientIP = val
			}
		} else if equalFoldBytes(key, []byte("user-agent")) {
			req.UserAgent = val
		} else if equalFoldBytes(key, []byte("accept")) {
			req.Accept = val
		} else if equalFoldBytes(key, []byte("x-tls-hash")) {
			req.TLSHash = val
		} else if equalFoldBytes(key, []byte("sec-ch-ua")) {
			req.SecCHUA = val
		} else if equalFoldBytes(key, []byte("accept-language")) {
			req.AcceptLang = val
		}
	}

	if req.HasContentLength && int64(req.ContentLength) > h.cfg.MaxRequestBodySize {
		return 0, req, errPayloadTooLarge
	}

	totalLen := idx + req.ContentLength
	if len(data) < totalLen {
		return 0, req, errIncompleteRequest
	}
	req.Body = data[idx : idx+req.ContentLength]
	return totalLen, req, nil
}

// React handles a parsed /track POST after filtering and audit logging.
func (h *AdsPacketHandler) React(req parsedHTTPRequest, c gnet.Conn) gnet.Action {
	ctx, ok := c.Context().(*connContext)
	if !ok {
		ctx = &connContext{
			pbReq: pb.AdEvent{
				Metadata: &pb.EventMetadata{},
			},
			trackReq: TrackRequest{
				Payload: make([]byte, 0, 512),
			},
			evt: domain.Event{
				Payload: make([]byte, 0, 1024),
			},
			valSlice: make([]any, 18),
			resp:     pb.TrackResponse{},
			bufSlice: make([]byte, 4096),
			wReqID: filter.BufWrapper{
				Buf: make([]byte, 0, 128),
			},
			wCamp: filter.BufWrapper{
				Buf: make([]byte, 0, 128),
			},
			wTime: filter.BufWrapper{
				Buf: make([]byte, 0, 128),
			},
		}
		if h.logger != nil {
			ctx.shardID = int(h.loggerShardCounter.Add(1) % uint64(len(h.logger.Shards())))
		}
		c.SetContext(ctx)
	}

	if len(req.Method) == 3 && req.Method[0] == 'G' && req.Method[1] == 'E' && req.Method[2] == 'T' {
		if bytes.HasPrefix(req.Path, []byte("/health")) {
			healthy := h.healthy.Load() == 1
			body := "OK"
			if !healthy {
				body = "DEGRADED"
			}
			if len(h.rdbsHealthy) > 0 {
				body += " redis="
				for i := range h.rdbsHealthy {
					if i > 0 {
						body += ","
					}
					st := h.rdbsHealthy[i].Load()
					body += strconv.Itoa(i) + ":" + statusStrings[st]
				}
			}
			statusLine := "HTTP/1.1 200 OK\r\n"
			if !healthy {
				statusLine = "HTTP/1.1 503 Service Unavailable\r\n"
			}
			hdr := statusLine + "Content-Type: text/plain\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\nConnection: keep-alive\r\n\r\n"
			out := make([]byte, len(hdr)+len(body))
			copy(out, hdr)
			copy(out[len(hdr):], body)
			h.write(c, out, ctx)
			return gnet.None
		}
		if bytes.Equal(req.Path, []byte("/metrics")) {
			h.write(c, filter.RespNotFound, ctx)
			return gnet.None
		}
		h.write(c, filter.RespMethodNotAllowed, ctx)
		return gnet.None
	}

	isPOST := len(req.Method) == 4 && req.Method[0] == 'P' && req.Method[1] == 'O' && req.Method[2] == 'S' && req.Method[3] == 'T'
	if !isPOST {
		h.write(c, filter.RespMethodNotAllowed, ctx)
		return gnet.None
	}

	if !bytes.Equal(req.Path, []byte("/track")) {
		h.write(c, filter.RespNotFound, ctx)
		return gnet.None
	}

	if !req.HasContentLength {
		h.write(c, filter.RespBadRequestClose, ctx)
		return gnet.Close
	}

	startMono := filter.MonotonicNano()

	ip := extractClientIPGnet(ctx, &req, c, h.cfg.TrustedProxies)
	ua := repo.UnsafeString(req.UserAgent)

	id, _ := clock.NewFastUUID()

	wReqID := &ctx.wReqID
	wReqID.Buf = wReqID.Buf[:0]
	wReqID.Buf = repo.AppendUUID(wReqID.Buf, id)

	fields, badResp, status, ok := h.parseTrackIngest(ctx, req, wReqID)
	if !ok {
		h.write(c, badResp, ctx)
		h.recordMetrics(startMono, status)
		return gnet.None
	}

	var requestIDStr string
	if fields.clickID == "" {
		requestIDStr = repo.UnsafeString(wReqID.Buf)
		fields.clickID = requestIDStr
	}

	evt := &ctx.evt
	fillTrackEvent(evt, fields, ip, ua)
	evt.TLSHash = repo.UnsafeString(req.TLSHash)
	evt.SecCHUA = repo.UnsafeString(req.SecCHUA)
	evt.AcceptLang = repo.UnsafeString(req.AcceptLang)

	if h.filterEngine != nil {
		outcome := processTrack(h.trackProc, evt, fields.deviceType)
		return h.deliverGnetTrack(ctx, req, c, evt, startMono, wReqID, requestIDStr, outcome)
	}

	h.trackMetrics.decisionAccepted.Inc()
	processor.WriteAuditLog(h.logger, &h.auditLogSeq, h.auditLogSampleMask, ctx.shardID, evt)
	landing := filter.ResolveLandingURL(h.registry, h.creativeStore, &ctx.evt)
	h.writeGnetTrackAccepted(ctx, req, c, startMono, wReqID, requestIDStr, landing)
	return gnet.None
}

// extractClientIPGnet resolves client IP from gnet request headers and cached remote addr.
func extractClientIPGnet(ctx *connContext, req *parsedHTTPRequest, c gnet.Conn, trustedProxies []string) string {
	if ctx.remoteIP == "" {
		ctx.remoteIP = getIPOnly(c.RemoteAddr().String())
	}
	remoteIP := ctx.remoteIP
	if !isTrustedProxy(remoteIP, trustedProxies) {
		return remoteIP
	}

	if len(req.ClientIP) > 0 {
		xff := repo.UnsafeString(req.ClientIP)
		last := len(xff)
		for i := len(xff) - 1; i >= -1; i-- {
			if i == -1 || xff[i] == ',' {
				start := i + 1
				for start < last && xff[start] == ' ' {
					start++
				}
				end := last
				for end > start && xff[end-1] == ' ' {
					end--
				}

				if start < end {
					ipStr := xff[start:end]
					parsedIP := net.ParseIP(ipStr)
					if parsedIP != nil && !parsedIP.IsPrivate() && !parsedIP.IsLoopback() && !parsedIP.IsLinkLocalUnicast() {
						return ipStr
					}
				}
				last = i
			}
		}
	}

	return remoteIP
}

// trimSpaceBytes normalizes header values in-place on the zero-copy gnet parse buffer.
func trimSpaceBytes(b []byte) []byte {
	start := 0
	for start < len(b) && (b[start] == ' ' || b[start] == '\t') {
		start++
	}
	end := len(b)
	for end > start && (b[end-1] == ' ' || b[end-1] == '\t' || b[end-1] == '\r') {
		end--
	}
	return b[start:end]
}

// equalFoldBytes matches HTTP header names on the gnet path without allocating folded strings.
func equalFoldBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		c1 := a[i]
		c2 := b[i]
		if c1 >= 'A' && c1 <= 'Z' {
			c1 += 'a' - 'A'
		}
		if c2 >= 'A' && c2 <= 'Z' {
			c2 += 'a' - 'A'
		}
		if c1 != c2 {
			return false
		}
	}
	return true
}

// parseDecimal decodes Content-Length on the gnet path without importing strconv.
func parseDecimal(b []byte) int {
	val := 0
	for _, c := range b {
		if c >= '0' && c <= '9' {
			val = val*10 + int(c-'0')
		}
	}
	return val
}
