package management

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"espx/internal/ads"
	"espx/internal/config"
	"espx/internal/metrics"

	"github.com/jackc/pgx/v5/pgxpool"
)

// UDPControlServer publishes ingress epochs and answers CONFIG_REQUEST over UDP only.
type UDPControlServer struct {
	cfg       *config.Config
	pool      *pgxpool.Pool
	sharder   ads.Sharder
	conn      *net.UDPConn
	epoch     atomic.Int64
	numShards int
	trackers  []*net.UDPAddr
}

// NewUDPControlServer builds a management UDP control-plane publisher.
func NewUDPControlServer(cfg *config.Config, pool *pgxpool.Pool, sharder ads.Sharder, numShards int) *UDPControlServer {
	s := &UDPControlServer{
		cfg:       cfg,
		pool:      pool,
		sharder:   sharder,
		numShards: numShards,
	}
	for _, raw := range cfg.UDPTrackerAddrs {
		if addr, err := net.ResolveUDPAddr("udp", raw); err == nil {
			s.trackers = append(s.trackers, addr)
		}
	}
	return s
}

// Start listens for CONFIG_REQUEST and publishes periodic epochs.
func (s *UDPControlServer) Start(ctx context.Context) error {
	if s == nil || s.cfg == nil || !s.cfg.UDPControlEnabled {
		return nil
	}
	addr, err := net.ResolveUDPAddr("udp", s.cfg.UDPMgmtBindAddr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	s.conn = conn
	go s.recvLoop(ctx)
	go s.publishLoop(ctx)
	slog.Info("management udp control started", "bind", addr.String(), "trackers", len(s.trackers))
	return nil
}

// Close shuts down the UDP socket.
func (s *UDPControlServer) Close() error {
	if s != nil && s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

func (s *UDPControlServer) recvLoop(ctx context.Context) {
	buf := make([]byte, 2048)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		_ = s.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, remote, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			continue
		}
		var hdr ads.UDPHeader
		if !ads.DecodeUDPHeader(buf[:n], &hdr) {
			continue
		}
		if hdr.MsgType != ads.UDPMsgConfigRequest {
			continue
		}
		payload := buf[ads.UDPHeaderSize:n]
		var req ads.UDPConfigRequestPayload
		if !ads.DecodeUDPConfigRequest(payload, &req) {
			continue
		}
		slog.Debug("udp config request", "tracker", req.TrackerID, "last_epoch", req.LastEpoch, "remote", remote)
		s.sendSnapshotBurst(ctx, remote, 5)
	}
}

func (s *UDPControlServer) publishLoop(ctx context.Context) {
	interval := time.Duration(s.cfg.UDPSyncIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	s.publishEpoch(ctx, false)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.publishEpoch(ctx, false)
		}
	}
}

func (s *UDPControlServer) publishEpoch(ctx context.Context, snapshot bool) {
	limits := s.buildLimits()
	epoch := s.epoch.Add(1)
	slotVersion := int32(0)
	if sh, ok := s.sharder.(*ads.StaticSlotSharder); ok {
		slotVersion = sh.SnapshotVersion()
	}
	hash := ads.ComputeUDPConfigHash(epoch, slotVersion, limits)
	if err := s.persistEpoch(ctx, epoch, hash, slotVersion, limits); err != nil {
		slog.Warn("control_plane_epochs insert failed", "error", err)
	}
	msgType := ads.UDPMsgQuotaEpoch
	flags := uint16(0)
	if snapshot {
		msgType = ads.UDPMsgConfigSnapshot
		flags = ads.UDPFlagSnapshot
	}
	hdr := &ads.UDPHeader{
		CoarseTimeNs:   time.Now().UnixNano(),
		EpochID:        epoch,
		ConfigHash:     hash,
		SlotMapVersion: slotVersion,
		Flags:          flags,
	}
	var pkt [512]byte
	n := ads.EncodeQuotaEpochDatagram(pkt[:], msgType, hdr, limits)
	if n == 0 {
		return
	}
	for _, taddr := range s.trackers {
		for i := 0; i < 3; i++ {
			_, _ = s.conn.WriteToUDP(pkt[:n], taddr)
		}
	}
	// broadcast to wildcard tracker port on localhost
	if bcast, err := net.ResolveUDPAddr("udp", "127.0.0.1:8191"); err == nil {
		_, _ = s.conn.WriteToUDP(pkt[:n], bcast)
	}
	metrics.UDPControlPublishTotal.Inc()
}

func (s *UDPControlServer) sendSnapshotBurst(ctx context.Context, addr *net.UDPAddr, count int) {
	limits := s.buildLimits()
	epoch := s.epoch.Load()
	if epoch == 0 {
		s.publishEpoch(ctx, true)
		epoch = s.epoch.Load()
	}
	slotVersion := int32(0)
	if sh, ok := s.sharder.(*ads.StaticSlotSharder); ok {
		slotVersion = sh.SnapshotVersion()
	}
	hash := ads.ComputeUDPConfigHash(epoch, slotVersion, limits)
	hdr := &ads.UDPHeader{
		CoarseTimeNs:   time.Now().UnixNano(),
		EpochID:        epoch,
		ConfigHash:     hash,
		SlotMapVersion: slotVersion,
		Flags:          ads.UDPFlagSnapshot,
	}
	var pkt [512]byte
	n := ads.EncodeQuotaEpochDatagram(pkt[:], ads.UDPMsgConfigSnapshot, hdr, limits)
	if n == 0 {
		return
	}
	for i := 0; i < count; i++ {
		_, _ = s.conn.WriteToUDP(pkt[:n], addr)
	}
	metrics.UDPControlPublishTotal.Add(float64(count))
}

func (s *UDPControlServer) buildLimits() *ads.UDPControlLimits {
	n := s.numShards
	if n <= 0 {
		n = 1
	}
	if n > ads.UDPMaxControlShards {
		n = ads.UDPMaxControlShards
	}
	limits := &ads.UDPControlLimits{NumShards: uint8(n)}
	rps := s.cfg.UDPDefaultShardRPS
	if rps == 0 {
		rps = 50_000
	}
	for i := 0; i < n; i++ {
		limits.Limits[i] = rps
	}
	return limits
}

func (s *UDPControlServer) persistEpoch(ctx context.Context, epoch int64, hash [16]byte, slotVersion int32, limits *ads.UDPControlLimits) error {
	if s.pool == nil {
		return nil
	}
	payload, err := ads.MarshalEpochPayload(slotVersion, limits)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO control_plane_epochs (epoch_id, config_hash, payload_json)
		VALUES ($1, $2, $3::jsonb)
		ON CONFLICT (epoch_id) DO NOTHING`,
		epoch, hash[:], json.RawMessage(payload))
	return err
}
