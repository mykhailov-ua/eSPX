package ingestion

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"espx/internal/config"
	"espx/internal/metrics"
)

// UDPChannelState is the tracker control-channel health.
type UDPChannelState uint32

const (
	UDPChannelOK UDPChannelState = iota
	UDPChannelStale
)

type ingressSnapshot struct {
	epoch          int64
	configHash     [16]byte
	slotMapVersion int32
	limits         UDPControlLimits
}

var ingressSnapshotPool = sync.Pool{
	New: func() any {
		return &ingressSnapshot{}
	},
}

// UDPControl receives management quota epochs and exposes per-shard ingress limits.
type UDPControl struct {
	enabled        bool
	failClosed     bool
	trackerID      uint32
	bindAddr       string
	syncInterval   time.Duration
	mgmtAddr       *net.UDPAddr
	snapshot       atomic.Pointer[ingressSnapshot]
	quotaMap       atomic.Pointer[ingressQuotaMap]
	numWorkers     int
	channelState   atomic.Uint32
	currentEpoch   atomic.Int64
	lastPacketMono atomic.Int64
	knownHash      [16]byte
	conn           *net.UDPConn
	requestConn    *net.UDPConn
}

// UDPControlConfig wires tracker-side UDP ingress control.
type UDPControlConfig struct {
	Enabled      bool
	FailClosed   bool
	TrackerID    uint32
	BindAddr     string
	MgmtAddr     string
	SyncInterval time.Duration
	NumShards    int
	NumWorkers   int
	InitialRPS   uint64
}

// NewUDPControl builds an idle controller; call Start to open sockets.
func NewUDPControl(cfg UDPControlConfig) *UDPControl {
	c := &UDPControl{
		enabled:      cfg.Enabled,
		failClosed:   cfg.FailClosed,
		trackerID:    cfg.TrackerID,
		bindAddr:     cfg.BindAddr,
		syncInterval: cfg.SyncInterval,
		numWorkers:   cfg.NumWorkers,
	}
	if c.numWorkers <= 0 {
		c.numWorkers = 1
	}
	if c.numWorkers > maxIngressWorkers {
		c.numWorkers = maxIngressWorkers
	}
	if cfg.SyncInterval <= 0 {
		c.syncInterval = 10 * time.Second
	}
	if cfg.MgmtAddr != "" {
		if addr, err := net.ResolveUDPAddr("udp", cfg.MgmtAddr); err == nil {
			c.mgmtAddr = addr
		}
	}
	if cfg.Enabled && cfg.NumShards > 0 {
		seed := ingressSnapshotPool.Get().(*ingressSnapshot)
		seed.epoch = 0
		if cfg.NumShards > UDPMaxControlShards {
			cfg.NumShards = UDPMaxControlShards
		}
		seed.limits.NumShards = uint8(cfg.NumShards)
		rps := cfg.InitialRPS
		if rps == 0 {
			rps = 50_000
		}
		for i := 0; i < cfg.NumShards; i++ {
			seed.limits.Limits[i] = rps
		}
		seed.configHash = ComputeUDPConfigHash(0, 0, &seed.limits)
		c.knownHash = seed.configHash
		c.snapshot.Store(seed)
		if qm := buildIngressQuotaMap(0, &seed.limits, c.numWorkers); qm != nil {
			c.quotaMap.Store(qm)
		}
	}
	return c
}

// NewUDPControlFromConfig adapts service config for tracker startup.
func NewUDPControlFromConfig(cfg *config.Config, numShards int) *UDPControl {
	if cfg == nil || !cfg.UDPControlEnabled {
		return nil
	}
	return NewUDPControl(UDPControlConfig{
		Enabled:      true,
		FailClosed:   cfg.UDPFailClosed,
		TrackerID:    cfg.UDPTrackerID,
		BindAddr:     cfg.UDPTrackerBindAddr,
		MgmtAddr:     cfg.UDPMgmtAddr,
		SyncInterval: time.Duration(cfg.UDPSyncIntervalMs) * time.Millisecond,
		NumShards:    numShards,
		NumWorkers:   cfg.MaxWorkers,
		InitialRPS:   cfg.UDPDefaultShardRPS,
	})
}

// Start opens the recv socket and background stale/request loops.
func (c *UDPControl) Start(ctx context.Context) error {
	if c == nil || !c.enabled {
		return nil
	}
	bind := ":8191"
	if c.bindAddr != "" {
		bind = c.bindAddr
	}
	addr, err := net.ResolveUDPAddr("udp", bind)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	_ = conn.SetReadBuffer(1 << 20)
	c.conn = conn

	reqConn, err := net.DialUDP("udp", nil, c.mgmtAddr)
	if err == nil {
		c.requestConn = reqConn
	}
	go c.recvLoop(ctx)
	go c.staleLoop(ctx)
	slog.Info("udp control plane started", "bind", addr.String(), "mgmt", c.mgmtAddr)
	return nil
}

// Close releases UDP sockets.
func (c *UDPControl) Close() error {
	if c == nil {
		return nil
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
	if c.requestConn != nil {
		_ = c.requestConn.Close()
	}
	return nil
}

// ChannelState returns OK or STALE for metrics/health.
func (c *UDPControl) ChannelState() UDPChannelState {
	if c == nil {
		return UDPChannelOK
	}
	return UDPChannelState(c.channelState.Load())
}

// CurrentEpoch returns the last applied epoch id.
func (c *UDPControl) CurrentEpoch() int64 {
	if c == nil {
		return 0
	}
	return c.currentEpoch.Load()
}

// TryIngress performs a lock-free per-worker shard quota check (M5).
func (c *UDPControl) TryIngress(shard, workerID int) bool {
	if c == nil || !c.enabled {
		return true
	}
	m := c.quotaMap.Load()
	if m == nil {
		return true
	}
	if m.tryAcquire(shard, workerID) {
		metrics.UDPIngressAcquireTotal.Inc()
		return true
	}
	metrics.UDPIngressRejectTotal.Inc()
	return false
}

// ShardLimitRPS returns the active per-shard ingress limit (canary floor when STALE).
func (c *UDPControl) ShardLimitRPS(shard int) uint64 {
	if c == nil || shard < 0 || shard >= UDPMaxControlShards {
		return 0
	}
	snap := c.snapshot.Load()
	if snap == nil {
		return 0
	}
	if int(shard) >= int(snap.limits.NumShards) {
		return 0
	}
	return snap.limits.Limits[shard]
}

// ApplyPacket decodes and applies one datagram (recv goroutine; pooled snapshot swap).
func (c *UDPControl) ApplyPacket(buf []byte) bool {
	if c == nil || len(buf) < UDPHeaderSize {
		metrics.UDPControlCorruptTotal.Inc()
		return false
	}
	var hdr UDPHeader
	if !udpDecodeHeader(buf, &hdr) {
		metrics.UDPControlCorruptTotal.Inc()
		return false
	}
	payload := buf[UDPHeaderSize:]
	if int(hdr.PayloadLen) > len(payload) {
		metrics.UDPControlCorruptTotal.Inc()
		return false
	}
	payload = payload[:hdr.PayloadLen]
	applyUDPCoarseTime(hdr.CoarseTimeNs)

	switch hdr.MsgType {
	case UDPMsgQuotaEpoch, UDPMsgConfigSnapshot:
		var limits UDPControlLimits
		if !udpDecodeShardLimits(payload, hdr.NumShards, &limits) {
			metrics.UDPControlCorruptTotal.Inc()
			return false
		}
		isSnapshot := hdr.MsgType == UDPMsgConfigSnapshot || hdr.Flags&UDPFlagSnapshot != 0
		return c.applyLimits(&hdr, &limits, isSnapshot)
	case UDPMsgMigrationBarrier:
		// M1 fences handle migration_gen in Lua; barrier updates slot-map watcher path later.
		c.markFresh()
		return true
	default:
		metrics.UDPControlCorruptTotal.Inc()
		return false
	}
}

func (c *UDPControl) applyLimits(hdr *UDPHeader, limits *UDPControlLimits, isSnapshot bool) bool {
	cur := c.currentEpoch.Load()
	epoch := hdr.EpochID

	if epoch <= cur {
		metrics.UDPControlStaleDropTotal.Inc()
		return false
	}

	if epoch == cur+1 || cur == 0 {
		c.commitSnapshot(hdr, limits)
		c.currentEpoch.Store(epoch)
		c.markFresh()
		if isSnapshot {
			metrics.UDPControlSnapshotAppliedTotal.Inc()
		}
		return true
	}

	// Epoch gap.
	prev := c.snapshot.Load()
	tightening := udpLimitsTightening(&prev.limits, limits)
	if tightening {
		c.commitSnapshot(hdr, limits)
		c.currentEpoch.Store(epoch)
		c.markFresh()
		metrics.UDPControlGapTightenTotal.Inc()
		return true
	}

	if isSnapshot {
		c.commitSnapshot(hdr, limits)
		c.currentEpoch.Store(epoch)
		c.markFresh()
		c.knownHash = hdr.ConfigHash
		metrics.UDPControlSnapshotAppliedTotal.Inc()
		return true
	}

	metrics.UDPControlLoosenBlockedTotal.Inc()
	return false
}

func (c *UDPControl) commitSnapshot(hdr *UDPHeader, limits *UDPControlLimits) {
	next := ingressSnapshotPool.Get().(*ingressSnapshot)
	next.epoch = hdr.EpochID
	next.configHash = hdr.ConfigHash
	next.slotMapVersion = hdr.SlotMapVersion
	next.limits = *limits
	old := c.snapshot.Swap(next)
	if old != nil {
		ingressSnapshotPool.Put(old)
	}
	if qm := buildIngressQuotaMap(hdr.EpochID, limits, c.numWorkers); qm != nil {
		oldMap := c.quotaMap.Swap(qm)
		if oldMap != nil {
			oldMap.cells = oldMap.cells[:0]
			ingressQuotaMapPool.Put(oldMap)
		}
	}
}

func (c *UDPControl) markFresh() {
	c.lastPacketMono.Store(monotonicNano())
	if c.channelState.Swap(uint32(UDPChannelOK)) == uint32(UDPChannelStale) {
		metrics.UDPControlRecoveredTotal.Inc()
	}
}

func (c *UDPControl) recvLoop(ctx context.Context) {
	buf := make([]byte, 2048)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		_ = c.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, err := c.conn.Read(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			continue
		}
		if n > 0 {
			metrics.UDPControlPacketsReceivedTotal.Inc()
			if c.ApplyPacket(buf[:n]) {
				metrics.UDPControlPacketsAppliedTotal.Inc()
			}
		}
	}
}

func (c *UDPControl) staleLoop(ctx context.Context) {
	ticker := time.NewTicker(c.syncInterval / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.checkStale()
		}
	}
}

func (c *UDPControl) checkStale() {
	last := c.lastPacketMono.Load()
	if last == 0 {
		return
	}
	threshold := c.syncInterval.Nanoseconds() * 2
	if monotonicNano()-last <= threshold {
		return
	}
	if c.channelState.Swap(uint32(UDPChannelStale)) != uint32(UDPChannelStale) {
		metrics.UDPControlStaleTotal.Inc()
		c.tightenCanaryFloor()
		c.sendConfigRequest()
	}
}

func (c *UDPControl) tightenCanaryFloor() {
	snap := c.snapshot.Load()
	if snap == nil {
		return
	}
	var limits UDPControlLimits
	limits.NumShards = snap.limits.NumShards
	limits.Limits = snap.limits.Limits
	udpApplyCanaryFloor(&limits)
	hdr := UDPHeader{
		Magic:          udpMagic,
		Version:        udpProtocolVersion,
		MsgType:        UDPMsgConfigSnapshot,
		Flags:          UDPFlagSnapshot,
		EpochID:        snap.epoch,
		ConfigHash:     snap.configHash,
		SlotMapVersion: snap.slotMapVersion,
		NumShards:      limits.NumShards,
	}
	c.commitSnapshot(&hdr, &limits)
}

func (c *UDPControl) sendConfigRequest() {
	if c.requestConn == nil || c.mgmtAddr == nil {
		return
	}
	snap := c.snapshot.Load()
	var hash [16]byte
	epoch := c.currentEpoch.Load()
	if snap != nil {
		hash = snap.configHash
	}
	var buf [UDPHeaderSize + 28]byte
	hdr := UDPHeader{
		Magic:      udpMagic,
		Version:    udpProtocolVersion,
		MsgType:    UDPMsgConfigRequest,
		EpochID:    epoch,
		ConfigHash: hash,
		PayloadLen: 28,
	}
	udpEncodeHeader(buf[:], &hdr)
	req := UDPConfigRequestPayload{TrackerID: c.trackerID, LastEpoch: epoch, Hash: hash}
	udpEncodeConfigRequest(buf[UDPHeaderSize:], &req)
	_, _ = c.requestConn.Write(buf[:])
	metrics.UDPControlConfigRequestTotal.Inc()
}

// EncodeQuotaEpochDatagram writes a QUOTA_EPOCH or CONFIG_SNAPSHOT datagram into dst.
func EncodeQuotaEpochDatagram(dst []byte, msgType uint8, hdr *UDPHeader, limits *UDPControlLimits) int {
	if hdr == nil || limits == nil {
		return 0
	}
	hdr.Magic = udpMagic
	hdr.Version = udpProtocolVersion
	hdr.MsgType = msgType
	hdr.NumShards = limits.NumShards
	payloadLen := udpShardPayloadLen(limits.NumShards)
	hdr.PayloadLen = uint16(payloadLen)
	if len(dst) < UDPHeaderSize+payloadLen {
		return 0
	}
	udpEncodeHeader(dst, hdr)
	return UDPHeaderSize + udpEncodeShardLimits(dst[UDPHeaderSize:], limits)
}

// EpochPayloadJSON is persisted in control_plane_epochs.payload_json.
type EpochPayloadJSON struct {
	ShardLimits    []uint64  `json:"shard_limits_rps"`
	SlotMapVersion int32     `json:"slot_map_version"`
	KState         []float64 `json:"k_state,omitempty"`
}

// MarshalEpochPayload serializes limits for Postgres audit/recovery.
func MarshalEpochPayload(slotVersion int32, limits *UDPControlLimits) ([]byte, error) {
	if limits == nil {
		return []byte("{}"), nil
	}
	p := EpochPayloadJSON{
		SlotMapVersion: slotVersion,
		ShardLimits:    make([]uint64, limits.NumShards),
	}
	for i := uint8(0); i < limits.NumShards; i++ {
		p.ShardLimits[i] = limits.Limits[i]
	}
	return json.Marshal(p)
}
