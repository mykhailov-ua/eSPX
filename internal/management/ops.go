package management

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"espx/internal/ads"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// RegisterOpsRoutes mounts unauthenticated health and metrics endpoints for orchestration probes.
func RegisterOpsRoutes(mux *http.ServeMux, pool *pgxpool.Pool, rdbs []redis.UniversalClient) {
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		if err := pool.Ping(ctx); err != nil {
			http.Error(w, "postgres unreachable", http.StatusServiceUnavailable)
			return
		}
		for i, rdb := range rdbs {
			if err := rdb.Ping(ctx).Err(); err != nil {
				http.Error(w, "redis shard unreachable", http.StatusServiceUnavailable)
				_ = i
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /ops/shards/slot-map", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		repo := ads.NewSlotMapRepo(pool)
		active, err := repo.GetActiveVersion(ctx)
		if err != nil {
			http.Error(w, "slot map meta unavailable", http.StatusServiceUnavailable)
			return
		}
		rows, err := repo.ListVersion(ctx, active)
		if err != nil {
			http.Error(w, "slot map unavailable", http.StatusServiceUnavailable)
			return
		}
		slots, err := ads.SlotMapShardTable(rows)
		if err != nil {
			http.Error(w, "slot map incomplete", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ads.OpsSlotMapResponse{
			Version:       active,
			ActiveVersion: active,
			Slots:         slots,
		})
	})
}
