// Package fingerprint stores passive XDP SYN TCP fingerprints for IVT correlation.
package fingerprint

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	redisRecentKey = "edge:tcp_fp:recent"
	redisByIPKey   = "edge:tcp_fp:ip:"
	defaultTTL     = time.Hour
)

// Entry is one observed SYN fingerprint for an IP.
type Entry struct {
	IP      string
	TCPHash uint32
	TTL     uint8
	Window  uint16
	MSS     uint8
	SeenAt  time.Time
}

// Record stores or refreshes a fingerprint observation for an IP.
func Record(ctx context.Context, rdb redis.Cmdable, e Entry) error {
	if rdb == nil || e.IP == "" {
		return nil
	}
	score := float64(e.SeenAt.Unix())
	member := fmt.Sprintf("%s:%08x", e.IP, e.TCPHash)
	pipe := rdb.Pipeline()
	pipe.ZAdd(ctx, redisRecentKey, redis.Z{Score: score, Member: member})
	pipe.HSet(ctx, redisByIPKey+e.IP,
		"tcp_hash", fmt.Sprintf("%08x", e.TCPHash),
		"ttl", strconv.Itoa(int(e.TTL)),
		"window", strconv.Itoa(int(e.Window)),
		"mss", strconv.Itoa(int(e.MSS)),
		"seen_at", strconv.FormatInt(e.SeenAt.Unix(), 10),
	)
	pipe.Expire(ctx, redisByIPKey+e.IP, defaultTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// ListRecent returns the most recent fingerprint entries from the staging ZSET.
func ListRecent(ctx context.Context, rdb redis.Cmdable, limit int) ([]Entry, error) {
	if rdb == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 256
	}
	members, err := rdb.ZRevRange(ctx, redisRecentKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Entry, 0, len(members))
	for _, member := range members {
		ip, hash, ok := parseMember(member)
		if !ok {
			continue
		}
		out = append(out, Entry{IP: ip, TCPHash: hash, SeenAt: time.Now().UTC()})
	}
	return out, nil
}

func parseMember(member string) (ip string, hash uint32, ok bool) {
	idx := strings.LastIndex(member, ":")
	if idx <= 0 || idx >= len(member)-1 {
		return "", 0, false
	}
	ip = member[:idx]
	v, err := strconv.ParseUint(member[idx+1:], 16, 32)
	if err != nil {
		return "", 0, false
	}
	return ip, uint32(v), true
}
