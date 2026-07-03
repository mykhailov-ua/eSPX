package management

import (
	"encoding/json"
	"net/http"
	"strconv"

	"espx/internal/ads/db"
	"espx/internal/ads/sharding"
	"espx/pkg/cold"
	"espx/pkg/httpresponse"

	"github.com/google/uuid"
)

func (h *Handler) registerSlotMapRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /admin/shards/slot-map", h.limit(h.perm(h.getSlotMap, PermShardsRead)))
	mux.HandleFunc("GET /admin/shards/slot-map/versions/{version}/migrations", h.limit(h.perm(h.getSlotMigrations, PermShardsRead)))
	mux.HandleFunc("POST /admin/shards/slot-map/versions", h.limit(h.perm(h.createSlotMapVersion, PermShardsWrite)))
	mux.HandleFunc("POST /admin/shards/slot-map/versions/{version}/migrate", h.limit(h.perm(h.markSlotMapMigrating, PermShardsWrite)))
	mux.HandleFunc("POST /admin/shards/slot-map/versions/{version}/copy", h.limit(h.perm(h.copySlotMigration, PermShardsWrite)))
	mux.HandleFunc("POST /admin/shards/slot-map/versions/{version}/activate", h.limit(h.perm(h.activateSlotMapVersion, PermShardsWrite)))
	mux.HandleFunc("POST /admin/shards/slot-map/rollback", h.limit(h.perm(h.rollbackSlotMap, PermShardsWrite)))
}

func (h *Handler) getSlotMap(w http.ResponseWriter, r *http.Request) {
	var version *int32
	if vStr := r.URL.Query().Get("version"); vStr != "" {
		parsed, err := strconv.ParseInt(vStr, 10, 32)
		if err != nil {
			httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid version query param")
			return
		}
		v := int32(parsed)
		version = &v
	}
	includeSlots := r.URL.Query().Get("include_slots") == "true" || r.URL.Query().Get("include_slots") == "1"

	dto, err := h.svc.GetSlotMap(r.Context(), version, includeSlots)
	if err != nil {
		writeSlotMapError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, dto)
}

func (h *Handler) createSlotMapVersion(w http.ResponseWriter, r *http.Request) {
	body, err := cold.ReadLimitedBody(w, r, cold.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	var req struct {
		BaseVersion *int32 `json:"base_version"`
		Overrides   []struct {
			Slot    int16  `json:"slot"`
			ShardID int16  `json:"shard_id"`
			State   string `json:"state"`
		} `json:"overrides"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid JSON")
		return
	}

	overrides := make([]sharding.SlotOverride, 0, len(req.Overrides))
	for _, o := range req.Overrides {
		state := db.RedisSlotStateACTIVE
		if o.State != "" {
			state = db.RedisSlotState(o.State)
		}
		overrides = append(overrides, sharding.SlotOverride{
			Slot:    o.Slot,
			ShardID: o.ShardID,
			State:   state,
		})
	}

	adminID := uuid.Nil
	if u, ok := GetUser(r.Context()); ok {
		adminID = u.UserID
	}

	newVersion, err := h.svc.CreateSlotMapVersion(r.Context(), adminID, req.BaseVersion, overrides)
	if err != nil {
		writeSlotMapError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusCreated, map[string]any{
		"version": newVersion,
	})
}

func (h *Handler) markSlotMapMigrating(w http.ResponseWriter, r *http.Request) {
	version, err := parsePathInt32(r, "version")
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid version")
		return
	}

	body, err := cold.ReadLimitedBody(w, r, cold.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	var req struct {
		Slots       []int16 `json:"slots"`
		TargetShard int16   `json:"target_shard"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid JSON")
		return
	}
	if len(req.Slots) == 0 {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "slots required")
		return
	}

	adminID := uuid.Nil
	if u, ok := GetUser(r.Context()); ok {
		adminID = u.UserID
	}

	if err := h.svc.MarkSlotMapMigrating(r.Context(), adminID, version, req.Slots, req.TargetShard); err != nil {
		writeSlotMapError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) activateSlotMapVersion(w http.ResponseWriter, r *http.Request) {
	version, err := parsePathInt32(r, "version")
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid version")
		return
	}

	adminID := uuid.Nil
	if u, ok := GetUser(r.Context()); ok {
		adminID = u.UserID
	}

	if err := h.svc.ActivateSlotMapVersion(r.Context(), adminID, version); err != nil {
		writeSlotMapError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) getSlotMigrations(w http.ResponseWriter, r *http.Request) {
	version, err := parsePathInt32(r, "version")
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid version")
		return
	}
	rows, err := h.svc.GetSlotMigrations(r.Context(), version)
	if err != nil {
		writeSlotMapError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]any{"migrations": rows})
}

func (h *Handler) copySlotMigration(w http.ResponseWriter, r *http.Request) {
	version, err := parsePathInt32(r, "version")
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid version")
		return
	}
	if err := h.svc.CopyAllMigratingSlots(r.Context(), version); err != nil {
		writeSlotMapError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) rollbackSlotMap(w http.ResponseWriter, r *http.Request) {
	body, err := cold.ReadLimitedBody(w, r, cold.DefaultMaxBody)
	if err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid request body")
		return
	}
	var req struct {
		PreviousVersion int32 `json:"previous_version"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", "invalid JSON")
		return
	}
	adminID := uuid.Nil
	if u, ok := GetUser(r.Context()); ok {
		adminID = u.UserID
	}
	if err := h.svc.RollbackSlotMapVersion(r.Context(), adminID, req.PreviousVersion); err != nil {
		writeSlotMapError(w, err)
		return
	}
	httpresponse.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeSlotMapError(w http.ResponseWriter, err error) {
	switch err {
	case sharding.ErrSlotMapVersionNotFound:
		httpresponse.Error(w, http.StatusNotFound, "NOT_FOUND", err.Error())
	case sharding.ErrSlotMapIncomplete, sharding.ErrSlotMapInvalidSlot, sharding.ErrSlotMapInvalidShard:
		httpresponse.Error(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
	case ErrSlotMigrationNotReady, sharding.ErrSlotMapAlreadyActive:
		httpresponse.Error(w, http.StatusConflict, "CONFLICT", err.Error())
	default:
		httpresponse.Error(w, http.StatusInternalServerError, "INTERNAL", err.Error())
	}
}

func parsePathInt32(r *http.Request, name string) (int32, error) {
	vStr := r.PathValue(name)
	parsed, err := strconv.ParseInt(vStr, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(parsed), nil
}
