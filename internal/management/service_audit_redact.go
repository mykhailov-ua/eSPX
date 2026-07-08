package management

import (
	"context"
	"encoding/json"
	"net"
	"regexp"
	"strings"

	"espx/internal/ads/db"

	"github.com/google/uuid"
)

var emailPIIPattern = regexp.MustCompile(`(?i)[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}`)

// AuditLogDTO is the API view of an audit row with optional PII redaction (M6.5).
type AuditLogDTO struct {
	ID         int64           `json:"id"`
	AdminID    string          `json:"admin_id,omitempty"`
	Action     string          `json:"action"`
	TargetType string          `json:"target_type"`
	TargetID   string          `json:"target_id,omitempty"`
	Changes    json.RawMessage `json:"changes"`
	Metadata   json.RawMessage `json:"metadata"`
	CreatedAt  string          `json:"created_at"`
}

// ListAuditLogsRedacted returns audit rows with optional email/IP masking.
func (s *Service) ListAuditLogsRedacted(ctx context.Context, limit, offset int32, redactPII bool) ([]AuditLogDTO, int64, error) {
	rows, total, err := s.ListAuditLogs(ctx, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	out := make([]AuditLogDTO, len(rows))
	for i, row := range rows {
		out[i] = auditRowToDTO(row, redactPII)
	}
	return out, total, nil
}

func auditRowToDTO(row db.AdminAuditLog, redactPII bool) AuditLogDTO {
	changes := row.Changes
	metadata := row.Metadata
	if redactPII {
		changes = redactJSONPII(changes)
		metadata = redactJSONPII(metadata)
	}
	dto := AuditLogDTO{
		ID:         row.ID,
		Action:     row.Action,
		TargetType: row.TargetType,
		Changes:    changes,
		Metadata:   metadata,
	}
	if row.AdminID.Valid {
		dto.AdminID = uuid.UUID(row.AdminID.Bytes).String()
	}
	if row.TargetID.Valid {
		dto.TargetID = uuid.UUID(row.TargetID.Bytes).String()
	}
	if row.CreatedAt.Valid {
		dto.CreatedAt = row.CreatedAt.Time.UTC().Format("2006-01-02T15:04:05Z")
	}
	return dto
}

func redactJSONPII(raw []byte) []byte {
	if len(raw) == 0 {
		return raw
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return []byte(redactStringPII(string(raw)))
	}
	redactValuePII(&v)
	out, err := json.Marshal(v)
	if err != nil {
		return raw
	}
	return out
}

func redactValuePII(v *any) {
	switch val := (*v).(type) {
	case map[string]any:
		for k, child := range val {
			lk := strings.ToLower(k)
			switch {
			case lk == "email" || strings.Contains(lk, "email"):
				val[k] = "[REDACTED_EMAIL]"
			case lk == "ip" || lk == "ip_address" || strings.HasSuffix(lk, "_ip"):
				val[k] = "[REDACTED_IP]"
			default:
				childCopy := child
				redactValuePII(&childCopy)
				val[k] = childCopy
			}
		}
	case []any:
		for i := range val {
			redactValuePII(&val[i])
		}
	case string:
		*v = redactStringPII(val)
	}
}

func redactStringPII(s string) string {
	if net.ParseIP(s) != nil {
		return "[REDACTED_IP]"
	}
	if emailPIIPattern.MatchString(s) {
		return emailPIIPattern.ReplaceAllString(s, "[REDACTED_EMAIL]")
	}
	return s
}
