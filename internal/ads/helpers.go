package ads

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"espx/internal/ads/db"
	"espx/internal/ads/pb"
	"espx/internal/config"
	"espx/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// nodeID identifies this process in fast UUID generation.
var nodeID uint16

// idSequence is the per-process counter mixed into fast UUIDs.
var idSequence uint64

// cachedUnixMilli avoids time.Now syscalls for TTC and timestamp fields on the hot path.
var cachedUnixMilli atomic.Int64

// cachedUnixMilliAny mirrors cachedUnixMilli for zero-alloc Lua argv boxing.
var cachedUnixMilliAny atomic.Value

// cachedNowUTC holds wall time refreshed once per second for schedule and pacing checks.
var cachedNowUTC atomic.Pointer[time.Time]

// clockRefreshPaused freezes cached wall-clock updates for deterministic chaos tests.
var clockRefreshPaused atomic.Bool

// SetClockRefreshPaused stops background cachedUnixMilli/cachedNowUTC refresh (tests only).
func SetClockRefreshPaused(paused bool) {
	clockRefreshPaused.Store(paused)
}

// storeCachedNowUTC snapshots the current UTC instant for cached time readers.
func storeCachedNowUTC() {
	t := time.Now().UTC()
	cachedNowUTC.Store(&t)
}

// CachedTimeUTC returns wall time in UTC without a syscall on the filter hot path.
func CachedTimeUTC() time.Time {
	if p := cachedNowUTC.Load(); p != nil {
		return *p
	}
	return time.Now().UTC()
}

// CachedTimeIn converts the cached UTC instant into a campaign timezone.
func CachedTimeIn(loc *time.Location) time.Time {
	if loc == nil || loc == time.UTC {
		return CachedTimeUTC()
	}
	return CachedTimeUTC().In(loc)
}

// init seeds fast UUID node identity and starts background time refresh goroutines.
func init() {
	hostname, _ := os.Hostname()
	h := uint32(os.Getpid())
	for _, c := range hostname {
		h = h*31 + uint32(c)
	}
	nodeID = uint16(h ^ (h >> 16))

	cachedUnixMilli.Store(time.Now().UnixMilli())
	cachedUnixMilliAny.Store(cachedUnixMilli.Load())
	storeCachedNowUTC()
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			if clockRefreshPaused.Load() {
				continue
			}
			ms := time.Now().UnixMilli()
			cachedUnixMilli.Store(ms)
			cachedUnixMilliAny.Store(ms)
		}
	}()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if clockRefreshPaused.Load() {
				continue
			}
			storeCachedNowUTC()
		}
	}()
}

// NewFastUUID generates click IDs without crypto/rand or uuid library overhead.
func NewFastUUID() (uuid.UUID, error) {
	seq := atomic.AddUint64(&idSequence, 1)
	now := cachedUnixMilli.Load()

	var u uuid.UUID

	u[0] = byte(now >> 40)
	u[1] = byte(now >> 32)
	u[2] = byte(now >> 24)
	u[3] = byte(now >> 16)
	u[4] = byte(now >> 8)
	u[5] = byte(now)

	u[6] = byte(seq >> 48)
	u[7] = byte(seq >> 40)

	u[8] = byte(nodeID >> 8)
	u[9] = byte(nodeID)

	u[10] = byte(seq >> 40)
	u[11] = byte(seq >> 32)
	u[12] = byte(seq >> 24)
	u[13] = byte(seq >> 16)
	u[14] = byte(seq >> 8)
	u[15] = byte(seq)

	u[6] = (u[6] & 0x0f) | 0x70
	u[8] = (u[8] & 0x3f) | 0x80

	return u, nil
}

// ToUUID wraps a uuid.UUID for pgtype query parameters.
func ToUUID(u uuid.UUID) pgtype.UUID {
	return pgtype.UUID{Bytes: u, Valid: true}
}

// MicroUnitFactor converts dollar floats to micro-dollar integers.
const MicroUnitFactor = 1_000_000

// SliceToMap builds O(1) country lookup sets from string slices.
func SliceToMap(slice []string) map[string]struct{} {
	if slice == nil {
		return nil
	}
	m := make(map[string]struct{}, len(slice))
	for _, s := range slice {
		m[s] = struct{}{}
	}
	return m
}

// CampaignRepo loads campaigns and applies idempotent budget sync updates from Redis.
type CampaignRepo struct {
	queries db.Querier
}

// NewCampaignRepo wraps sqlc queries for campaign persistence.
func NewCampaignRepo(queries db.Querier) *CampaignRepo {
	return &CampaignRepo{queries: queries}
}

// GetByID loads full campaign fields for budget cache reload paths.
func (r *CampaignRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Campaign, error) {
	row, err := r.queries.GetCampaignFull(ctx, pgtype.UUID{Bytes: id, Valid: true})
	if err != nil {
		return nil, err
	}
	return campaignFromDBRow(row), nil
}

// UpdateStatus changes campaign lifecycle state in Postgres.
func (r *CampaignRepo) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.CampaignStatus) error {
	_, err := r.queries.UpdateCampaignStatus(ctx, db.UpdateCampaignStatusParams{
		ID:     pgtype.UUID{Bytes: id, Valid: true},
		Status: db.CampaignStatusType(status),
	})
	return err
}

// UpdateSpend applies a Redis sync delta exactly once per sync transaction id.
func (r *CampaignRepo) UpdateSpend(ctx context.Context, id uuid.UUID, amount int64, txID string) error {
	var dbtx db.DBTX
	if getter, ok := r.queries.(interface{ DB() db.DBTX }); ok {
		dbtx = getter.DB()
	}

	if dbtx == nil {
		return r.queries.UpdateCampaignSpend(ctx, db.UpdateCampaignSpendParams{
			ID:           pgtype.UUID{Bytes: id, Valid: true},
			CurrentSpend: amount,
		})
	}

	var tx pgx.Tx
	var err error
	if beginner, ok := dbtx.(interface {
		Begin(context.Context) (pgx.Tx, error)
	}); ok {
		tx, err = beginner.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
	}

	var exec db.DBTX = dbtx
	if tx != nil {
		exec = tx
	}

	tag, err := exec.Exec(ctx, "INSERT INTO sync_idempotency (id) VALUES ($1) ON CONFLICT DO NOTHING", txID)
	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		return nil
	}

	var q db.Querier = r.queries
	if tx != nil {
		if concreteQueries, ok := r.queries.(*db.Queries); ok {
			q = concreteQueries.WithTx(tx)
		}
	}

	err = q.UpdateCampaignSpend(ctx, db.UpdateCampaignSpendParams{
		ID:           pgtype.UUID{Bytes: id, Valid: true},
		CurrentSpend: amount,
	})
	if err != nil {
		return err
	}

	// Phase 1.5.1: decrease reserved_amount proportionally to spend flushed
	sharder := NewStaticSlotSharder(config.ExpectedRedisShardCount)
	shardID := int16(sharder.GetShard(id))
	_ = q.DecreaseCampaignQuotaReserved(ctx, db.DecreaseCampaignQuotaReservedParams{
		ShardID:        shardID,
		CampaignID:     pgtype.UUID{Bytes: id, Valid: true},
		ReservedAmount: amount,
	})

	if tx != nil {
		return tx.Commit(ctx)
	}
	return nil
}

// ListActive returns all active campaigns for reconciliation and admin paths.
func (r *CampaignRepo) ListActive(ctx context.Context) ([]*domain.Campaign, error) {
	rows, err := r.queries.ListActiveCampaigns(ctx)
	if err != nil {
		return nil, err
	}

	campaigns := make([]*domain.Campaign, len(rows))
	for i, row := range rows {
		campaigns[i] = campaignFromDBRow(row)
	}
	return campaigns, nil
}

// CustomerRepo loads customers and applies idempotent balance sync updates from Redis.
type CustomerRepo struct {
	queries db.Querier
}

// NewCustomerRepo wraps sqlc queries for customer persistence.
func NewCustomerRepo(queries db.Querier) *CustomerRepo {
	return &CustomerRepo{queries: queries}
}

// GetByID loads a customer record by primary key.
func (r *CustomerRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Customer, error) {
	row, err := r.queries.GetCustomerByID(ctx, pgtype.UUID{Bytes: id, Valid: true})
	if err != nil {
		return nil, err
	}

	return &domain.Customer{
		ID:       id,
		Name:     row.Name,
		Balance:  row.Balance,
		Currency: row.Currency,
	}, nil
}

// UpdateBalance applies a Redis sync delta exactly once per sync transaction id.
func (r *CustomerRepo) UpdateBalance(ctx context.Context, id uuid.UUID, amount int64, txID string) error {
	var dbtx db.DBTX
	if getter, ok := r.queries.(interface{ DB() db.DBTX }); ok {
		dbtx = getter.DB()
	}

	if dbtx == nil {
		return r.queries.UpdateCustomerBalance(ctx, db.UpdateCustomerBalanceParams{
			ID:      pgtype.UUID{Bytes: id, Valid: true},
			Balance: amount,
		})
	}

	var tx pgx.Tx
	var err error
	if beginner, ok := dbtx.(interface {
		Begin(context.Context) (pgx.Tx, error)
	}); ok {
		tx, err = beginner.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
	}

	var exec db.DBTX = dbtx
	if tx != nil {
		exec = tx
	}

	tag, err := exec.Exec(ctx, "INSERT INTO sync_idempotency (id) VALUES ($1) ON CONFLICT DO NOTHING", txID)
	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		return nil
	}

	var q db.Querier = r.queries
	if tx != nil {
		if concreteQueries, ok := r.queries.(*db.Queries); ok {
			q = concreteQueries.WithTx(tx)
		}
	}

	err = q.UpdateCustomerBalance(ctx, db.UpdateCustomerBalanceParams{
		ID:      pgtype.UUID{Bytes: id, Valid: true},
		Balance: amount,
	})
	if err != nil {
		return err
	}

	if tx != nil {
		return tx.Commit(ctx)
	}
	return nil
}

// UnsafeString views bytes as a string without copy when the backing slice outlives use.
func UnsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// UnsafeBytes views a string as bytes without copy when the string is not mutated.
func UnsafeBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// ByteSliceValue adapts a byte slice for Redis binary marshaling without allocation.
type ByteSliceValue struct {
	b []byte
}

// MarshalBinary returns the wrapped bytes for Redis stream values.
func (v *ByteSliceValue) MarshalBinary() ([]byte, error) {
	return v.b, nil
}

// byteSliceValuePool recycles ByteSliceValue wrappers for stream XADD calls.
var byteSliceValuePool = sync.Pool{
	New: func() any {
		return new(ByteSliceValue)
	},
}

// DeepResetAdStreamEvent clears slice fields in place before returning protobuf objects to a pool.
func DeepResetAdStreamEvent(m *pb.AdStreamEvent) {
	if m == nil {
		return
	}
	m.ClickId = m.ClickId[:0]
	m.CampaignId = m.CampaignId[:0]
	m.EventType = m.EventType[:0]
	m.Payload = m.Payload[:0]
	m.Ip = m.Ip[:0]
	m.Ua = m.Ua[:0]
	m.FraudReason = m.FraudReason[:0]
	m.CreatedAtUnix = 0
	m.FraudScore = 0
	m.GhostEvent = false
}

// ClearAdStreamEvent nils large byte fields so pooled protobuf objects do not pin payload memory.
func ClearAdStreamEvent(m *pb.AdStreamEvent) {
	if m == nil {
		return
	}
	m.ClickId = nil
	m.CampaignId = nil
	m.EventType = nil
	m.Payload = nil
	m.Ip = nil
	m.Ua = nil
	m.FraudReason = nil
	m.CreatedAtUnix = 0
	m.FraudScore = 0
	m.GhostEvent = false
}

// DeepResetAdDLQEvent clears nested stream events before returning DLQ protobuf objects to a pool.
func DeepResetAdDLQEvent(m *pb.AdDLQEvent) {
	if m == nil {
		return
	}
	if m.OriginalEvent != nil {
		DeepResetAdStreamEvent(m.OriginalEvent)
	}
	m.Error = m.Error[:0]
	m.OriginalId = m.OriginalId[:0]
	m.WorkerId = m.WorkerId[:0]
	m.FailedAtUnix = 0
	m.RetryCount = 0
}
