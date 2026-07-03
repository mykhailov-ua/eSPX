package cmd

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"espx/internal/ads"
	"espx/internal/ads/db"
	"espx/internal/config"
	"espx/internal/management"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

var slotMapCmd = &cobra.Command{
	Use:   "slot-map",
	Short: "Fixed Slot Map control plane (Phase 2.1)",
}

var slotMapShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show active or specific slot map version summary",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		repo := ads.NewSlotMapRepo(pool)
		active, err := repo.GetActiveVersion(ctx)
		if err != nil {
			return err
		}

		version := active
		if vStr, _ := cmd.Flags().GetString("version"); vStr != "" {
			parsed, err := strconv.ParseInt(vStr, 10, 32)
			if err != nil {
				return err
			}
			version = int32(parsed)
		}

		rows, err := repo.ListVersion(ctx, version)
		if err != nil {
			return err
		}
		migrating, err := repo.ListMigratingSlots(ctx, version)
		if err != nil {
			return err
		}

		fmt.Printf("active_version=%d shown_version=%d slots=%d migrating=%d\n",
			active, version, len(rows), len(migrating))
		if full, _ := cmd.Flags().GetBool("full"); full {
			for _, row := range rows {
				fmt.Printf("slot=%d shard=%d state=%s\n", row.Slot, row.ShardID, row.State)
			}
		}
		return nil
	},
}

var slotMapCreateVersionCmd = &cobra.Command{
	Use:   "create-version",
	Short: "Clone active map into a new version with optional slot overrides",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		overrideStrs, _ := cmd.Flags().GetStringSlice("override")
		overrides, err := parseSlotOverrides(overrideStrs)
		if err != nil {
			return err
		}

		repo := ads.NewSlotMapRepo(pool)
		active, err := repo.GetActiveVersion(ctx)
		if err != nil {
			return err
		}
		base := active
		if baseStr, _ := cmd.Flags().GetString("base-version"); baseStr != "" {
			parsed, err := strconv.ParseInt(baseStr, 10, 32)
			if err != nil {
				return err
			}
			base = int32(parsed)
		}

		newVersion, err := repo.CreateNextVersion(ctx, base, overrides)
		if err != nil {
			return err
		}
		fmt.Printf("created slot map version %d from base %d (overrides=%d)\n", newVersion, base, len(overrides))
		return nil
	},
}

var slotMapMarkMigratingCmd = &cobra.Command{
	Use:   "mark-migrating",
	Short: "Mark slots MIGRATING on a draft version",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		version, _ := cmd.Flags().GetInt32("version")
		targetShard, _ := cmd.Flags().GetInt16("target-shard")
		slotsStr, _ := cmd.Flags().GetString("slots")
		slots, err := parseSlotList(slotsStr)
		if err != nil {
			return err
		}

		repo := ads.NewSlotMapRepo(pool)
		if err := repo.MarkSlotsMigrating(ctx, version, slots, targetShard); err != nil {
			return err
		}
		fmt.Printf("marked %d slots MIGRATING on version %d -> shard %d\n", len(slots), version, targetShard)
		return nil
	},
}

var slotMapCopyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy Redis keys for all MIGRATING slots in a draft version",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		version, _ := cmd.Flags().GetInt32("version")
		rdbs, _, err := getRedisShards(ctx)
		if err != nil {
			return err
		}
		sharder := ads.NewStaticSlotSharder(len(rdbs))
		cfg, _ := config.Load()
		svc := management.NewService(pool, rdbs, sharder, cfg)
		defer svc.Close()
		if err := svc.CopyAllMigratingSlots(ctx, version); err != nil {
			return err
		}
		fmt.Printf("copied MIGRATING slots for version %d\n", version)
		return nil
	},
}

var slotMapMigrationsCmd = &cobra.Command{
	Use:   "migrations",
	Short: "Show slot migration progress for a version",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		version, _ := cmd.Flags().GetInt32("version")
		rdbs, _, err := getRedisShards(ctx)
		if err != nil {
			return err
		}
		cfg, _ := config.Load()
		svc := management.NewService(pool, rdbs, ads.NewStaticSlotSharder(len(rdbs)), cfg)
		defer svc.Close()
		rows, err := svc.GetSlotMigrations(ctx, version)
		if err != nil {
			return err
		}
		for _, row := range rows {
			fmt.Printf("slot=%d source=%d target=%d state=%s copied=%d/%d",
				row.Slot, row.SourceShard, row.TargetShard, row.State, row.CampaignsCopied, row.CampaignsTotal)
			if row.LastError != "" {
				fmt.Printf(" err=%q", row.LastError)
			}
			fmt.Println()
		}
		return nil
	},
}

var slotMapRollbackCmd = &cobra.Command{
	Use:   "rollback",
	Short: "Revert active_version to a previous map and broadcast reload",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		prev, _ := cmd.Flags().GetInt32("previous-version")
		rdbs, _, err := getRedisShards(ctx)
		if err != nil {
			return err
		}
		cfg, _ := config.Load()
		svc := management.NewService(pool, rdbs, ads.NewStaticSlotSharder(len(rdbs)), cfg)
		defer svc.Close()
		if err := svc.RollbackSlotMapVersion(ctx, uuid.Nil, prev); err != nil {
			return err
		}
		fmt.Printf("rolled back active slot map to version %d\n", prev)
		return nil
	},
}

var slotMapActivateCmd = &cobra.Command{
	Use:   "activate",
	Short: "Switch active_version pointer after validation",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		version, _ := cmd.Flags().GetInt32("version")
		repo := ads.NewSlotMapRepo(pool)
		if err := repo.ActivateVersion(ctx, version); err != nil {
			return err
		}
		fmt.Printf("activated slot map version %d\n", version)
		return nil
	},
}

var slotMapExplainCmd = &cobra.Command{
	Use:   "explain",
	Short: "Run EXPLAIN ANALYZE on control-plane queries (requires Postgres)",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := context.Background()
		pool, err := getDB(ctx)
		if err != nil {
			return err
		}
		defer pool.Close()

		queries := []struct {
			name string
			sql  string
		}{
			{
				name: "LockSlotMapMeta",
				sql:  "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT active_version FROM redis_slot_map_meta WHERE id = 1 FOR UPDATE",
			},
			{
				name: "CountSlotMapRowsForVersion",
				sql:  fmt.Sprintf("EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT COUNT(*) FROM redis_slot_map WHERE version = 1"),
			},
			{
				name: "LockSlotMapEntry",
				sql:  "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT version, slot, shard_id, state FROM redis_slot_map WHERE version = 1 AND slot = 42 FOR UPDATE",
			},
			{
				name: "ListMigratingSlotsByVersion",
				sql:  "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) SELECT version, slot, shard_id, state FROM redis_slot_map WHERE version = 1 AND state = 'MIGRATING' ORDER BY slot",
			},
			{
				name: "CopySlotMapVersion",
				sql:  "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) INSERT INTO redis_slot_map (version, slot, shard_id, state) SELECT 999, slot, shard_id, state FROM redis_slot_map WHERE version = 1",
			},
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()

		shards := config.ExpectedRedisShardCount
		for _, q := range queries {
			fmt.Printf("\n=== %s ===\n", q.name)
			rows, err := tx.Query(ctx, q.sql)
			if err != nil {
				return fmt.Errorf("%s: %w", q.name, err)
			}
			for rows.Next() {
				var line string
				if err := rows.Scan(&line); err != nil {
					rows.Close()
					return err
				}
				fmt.Println(line)
			}
			rows.Close()
		}
		fmt.Printf("\nexpected_shard_count=%d (StaticSlot default topology)\n", shards)
		return tx.Rollback(ctx)
	},
}

func parseSlotOverrides(specs []string) ([]ads.SlotOverride, error) {
	out := make([]ads.SlotOverride, 0, len(specs))
	for _, spec := range specs {
		parts := strings.Split(spec, ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("override must be slot:shard:state, got %q", spec)
		}
		slot, err := strconv.ParseInt(parts[0], 10, 16)
		if err != nil {
			return nil, err
		}
		shard, err := strconv.ParseInt(parts[1], 10, 16)
		if err != nil {
			return nil, err
		}
		out = append(out, ads.SlotOverride{
			Slot:    int16(slot),
			ShardID: int16(shard),
			State:   db.RedisSlotState(strings.ToUpper(parts[2])),
		})
	}
	return out, nil
}

func parseSlotList(s string) ([]int16, error) {
	if s == "" {
		return nil, fmt.Errorf("slots required")
	}
	parts := strings.Split(s, ",")
	out := make([]int16, 0, len(parts))
	for _, p := range parts {
		v, err := strconv.ParseInt(strings.TrimSpace(p), 10, 16)
		if err != nil {
			return nil, err
		}
		out = append(out, int16(v))
	}
	return out, nil
}

func init() {
	slotMapShowCmd.Flags().String("version", "", "Map version (default: active)")
	slotMapShowCmd.Flags().Bool("full", false, "Print all 1024 slots")

	slotMapCreateVersionCmd.Flags().String("base-version", "", "Base version (default: active)")
	slotMapCreateVersionCmd.Flags().StringSlice("override", nil, "slot:shard:state overrides (repeatable)")

	slotMapMarkMigratingCmd.Flags().Int32("version", 0, "Draft map version")
	slotMapMarkMigratingCmd.Flags().Int16("target-shard", 0, "Target shard for migrating slots")
	slotMapMarkMigratingCmd.Flags().String("slots", "", "Comma-separated slot indices")
	_ = slotMapMarkMigratingCmd.MarkFlagRequired("version")
	_ = slotMapMarkMigratingCmd.MarkFlagRequired("target-shard")
	_ = slotMapMarkMigratingCmd.MarkFlagRequired("slots")

	slotMapActivateCmd.Flags().Int32("version", 0, "Version to activate")
	_ = slotMapActivateCmd.MarkFlagRequired("version")

	slotMapCopyCmd.Flags().Int32("version", 0, "Draft map version with MIGRATING slots")
	_ = slotMapCopyCmd.MarkFlagRequired("version")

	slotMapMigrationsCmd.Flags().Int32("version", 0, "Map version")
	_ = slotMapMigrationsCmd.MarkFlagRequired("version")

	slotMapRollbackCmd.Flags().Int32("previous-version", 0, "Version to restore as active")
	_ = slotMapRollbackCmd.MarkFlagRequired("previous-version")

	slotMapCmd.AddCommand(slotMapShowCmd)
	slotMapCmd.AddCommand(slotMapCreateVersionCmd)
	slotMapCmd.AddCommand(slotMapMarkMigratingCmd)
	slotMapCmd.AddCommand(slotMapCopyCmd)
	slotMapCmd.AddCommand(slotMapMigrationsCmd)
	slotMapCmd.AddCommand(slotMapActivateCmd)
	slotMapCmd.AddCommand(slotMapRollbackCmd)
	slotMapCmd.AddCommand(slotMapExplainCmd)
	rootCmd.AddCommand(slotMapCmd)
}
