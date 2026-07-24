package ingestion

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"espx/internal/metrics"
)

// TCPControlClient pulls signed routing snapshots from management over TCP (M2).
type TCPControlClient struct {
	enabled    bool
	secret     []byte
	trackerID  uint32
	mgmtAddr   string
	dialTO     time.Duration
	sharder    *StaticSlotSharder
	udpControl *UDPControl
	lastEpoch  atomic.Int64
}

// TCPControlClientConfig wires tracker-side TCP snapshot pull.
type TCPControlClientConfig struct {
	Enabled   bool
	Secret    []byte
	TrackerID uint32
	MgmtAddr  string
	DialTO    time.Duration
	Sharder   *StaticSlotSharder
	UDP       *UDPControl
}

// NewTCPControlClient builds an idle TCP control client.
func NewTCPControlClient(cfg TCPControlClientConfig) *TCPControlClient {
	if cfg.DialTO <= 0 {
		cfg.DialTO = 3 * time.Second
	}
	return &TCPControlClient{
		enabled:    cfg.Enabled,
		secret:     cfg.Secret,
		trackerID:  cfg.TrackerID,
		mgmtAddr:   cfg.MgmtAddr,
		dialTO:     cfg.DialTO,
		sharder:    cfg.Sharder,
		udpControl: cfg.UDP,
	}
}

// RequestSnapshot dials management, receives a signed snapshot, applies it, and ACKs.
func (c *TCPControlClient) RequestSnapshot(ctx context.Context) error {
	if c == nil || !c.enabled || c.mgmtAddr == "" {
		return nil
	}
	dialer := net.Dialer{Timeout: c.dialTO}
	conn, err := dialer.DialContext(ctx, "tcp", c.mgmtAddr)
	if err != nil {
		metrics.TCPControlSnapshotErrorsTotal.Inc()
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(c.dialTO))

	var reqHdr TCPControlHeader
	reqHdr.MsgType = TCPMsgSnapshotRequest
	reqHdr.TrackerID = c.trackerID
	if c.sharder != nil {
		reqHdr.SlotMapVersion = c.sharder.ActiveVersion()
		reqHdr.RoutingEpoch = c.sharder.Snapshot().MigrationGen
	}
	var reqBuf [TCPControlHeaderSize]byte
	if _, err := EncodeTCPControlFrame(reqBuf[:], c.secret, &reqHdr, nil); err != nil {
		return err
	}
	if _, err := conn.Write(reqBuf[:]); err != nil {
		metrics.TCPControlSnapshotErrorsTotal.Inc()
		return err
	}

	var frame [4096]byte
	n, err := io.ReadAtLeast(conn, frame[:], TCPControlHeaderSize)
	if err != nil {
		metrics.TCPControlSnapshotErrorsTotal.Inc()
		return err
	}
	for n < cap(frame) {
		m, rerr := conn.Read(frame[n:])
		n += m
		if rerr != nil {
			if rerr == io.EOF {
				break
			}
			metrics.TCPControlSnapshotErrorsTotal.Inc()
			return rerr
		}
		if m == 0 {
			break
		}
	}
	var hdr TCPControlHeader
	payload, err := DecodeTCPControlFrame(frame[:n], c.secret, &hdr)
	if err != nil {
		metrics.TCPControlSnapshotErrorsTotal.Inc()
		return err
	}
	if hdr.MsgType != TCPMsgSnapshot {
		metrics.TCPControlSnapshotErrorsTotal.Inc()
		return ErrTCPControlCorrupt
	}

	var limits UDPControlLimits
	if hdr.NumShards > 0 && len(payload) > 0 {
		if !udpDecodeShardLimits(payload, hdr.NumShards, &limits) {
			metrics.TCPControlSnapshotErrorsTotal.Inc()
			return ErrTCPControlCorrupt
		}
	}
	c.applySnapshot(&hdr, &limits)
	if err := c.sendACK(conn, &hdr); err != nil {
		metrics.TCPControlSnapshotErrorsTotal.Inc()
		return err
	}
	metrics.TCPControlSnapshotAppliedTotal.Inc()
	slog.Info("tcp routing snapshot applied",
		"routing_epoch", hdr.RoutingEpoch,
		"slot_version", hdr.SlotMapVersion,
		"tracker_id", c.trackerID,
	)
	return nil
}

func (c *TCPControlClient) applySnapshot(hdr *TCPControlHeader, limits *UDPControlLimits) {
	if c.sharder != nil {
		prev := c.sharder.Snapshot()
		c.sharder.SwapSnapshot(hdr.SlotMapVersion, &prev.Table, hdr.RoutingEpoch)
	}
	if c.udpControl != nil && limits != nil && limits.NumShards > 0 {
		udpHdr := UDPHeader{
			EpochID:        hdr.RoutingEpoch,
			SlotMapVersion: hdr.SlotMapVersion,
			NumShards:      limits.NumShards,
		}
		c.udpControl.commitSnapshot(&udpHdr, limits)
		c.udpControl.currentEpoch.Store(hdr.RoutingEpoch)
		c.udpControl.markFresh()
	}
	c.lastEpoch.Store(hdr.RoutingEpoch)
}

func (c *TCPControlClient) sendACK(conn net.Conn, snap *TCPControlHeader) error {
	ack := TCPAckPayload{
		TrackerID:      c.trackerID,
		AppliedEpoch:   snap.RoutingEpoch,
		AppliedSlotVer: snap.SlotMapVersion,
	}
	var body [16]byte
	if EncodeTCPAckPayload(body[:], &ack) == 0 {
		return ErrTCPControlCorrupt
	}
	var hdr TCPControlHeader
	hdr.MsgType = TCPMsgAck
	hdr.TrackerID = c.trackerID
	hdr.RoutingEpoch = snap.RoutingEpoch
	hdr.SlotMapVersion = snap.SlotMapVersion
	var frame [80]byte
	n, err := EncodeTCPControlFrame(frame[:], c.secret, &hdr, body[:])
	if err != nil {
		return err
	}
	_, err = conn.Write(frame[:n])
	if err == nil {
		metrics.TCPControlAckSentTotal.Inc()
	}
	return err
}
