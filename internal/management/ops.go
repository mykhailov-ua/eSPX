package management

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"espx/internal/database"
	"espx/internal/health"
	"espx/internal/ingestion"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// RegisterOpsRoutes mounts unauthenticated health and metrics endpoints for orchestration probes.
func RegisterOpsRoutes(mux *http.ServeMux, pool *pgxpool.Pool, rdbs []redis.UniversalClient) {
	live := &health.Liveness{}
	ready := &health.ReadinessProbe{}
	ready.StartBackground(context.Background(), 2*time.Second, func(ctx context.Context) bool {
		if err := pool.Ping(ctx); err != nil {
			return false
		}
		for _, rdb := range rdbs {
			if err := rdb.Ping(ctx).Err(); err != nil {
				return false
			}
		}
		return true
	})
	health.Register(mux, live, ready)
	prometheus.MustRegister(database.NewPgTableStatsCollector(pool))
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		ready.ServeReadyz(w, r)
	})
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /ops/shards/slot-map", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		repo := ingestion.NewSlotMapRepo(pool)
		active, err := repo.GetActiveVersion(ctx)
		if err != nil {
			http.Error(w, "slot map meta unavailable", http.StatusServiceUnavailable)
			return
		}
		meta, _ := repo.GetSlotMapMeta(ctx)
		rows, err := repo.ListVersion(ctx, active)
		if err != nil {
			http.Error(w, "slot map unavailable", http.StatusServiceUnavailable)
			return
		}
		slots, err := ingestion.SlotMapShardTable(rows)
		if err != nil {
			http.Error(w, "slot map incomplete", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(ingestion.OpsSlotMapResponse{
			Version:       active,
			ActiveVersion: active,
			RoutingEpoch:  meta.RoutingEpoch,
			Slots:         slots,
		}); err != nil {
			slog.Error("ops slot map encode failed", "error", err)
		}
	})
}
