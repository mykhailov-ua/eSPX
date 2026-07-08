package notifier

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"espx/internal/notifier/pb"
)

// RenderTemplate substitutes {{var}} placeholders in a template body.
func RenderTemplate(body string, vars map[string]string) string {
	out := body
	for key, value := range vars {
		out = strings.ReplaceAll(out, "{{"+key+"}}", value)
	}
	return out
}

func (service *Service) resolveNotificationBody(ctx context.Context, req *pb.SendNotificationRequest) (string, error) {
	if req == nil {
		return "", ErrBodyRequired
	}
	if req.TemplateId == "" {
		if req.Body == "" {
			return "", ErrBodyRequired
		}
		return req.Body, nil
	}

	tmpl, err := service.queries.GetTemplate(ctx, req.TemplateId)
	if err != nil {
		return "", fmt.Errorf("load template %s: %w", req.TemplateId, err)
	}

	vars := make(map[string]string, len(req.TemplateVars)+2)
	for k, v := range req.TemplateVars {
		vars[k] = v
	}
	if req.AttachmentUrl != "" {
		vars["attachment_url"] = req.AttachmentUrl
	}
	if req.Title != "" {
		vars["title"] = req.Title
	}
	body := RenderTemplate(tmpl.Body, vars)
	if body == "" {
		return "", ErrBodyRequired
	}
	return body, nil
}

// RetryNotification resets a FAILED row to PENDING for operator replay.
func (service *Service) RetryNotification(ctx context.Context, notificationID string) (*pb.Notification, error) {
	id, err := pgUUIDFromString(notificationID)
	if err != nil {
		return nil, err
	}
	row, err := service.queries.RetryNotification(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("retry notification: %w", err)
	}
	return notificationToProto(row), nil
}

func marshalTemplateVarsJSON(vars map[string]string) ([]byte, error) {
	if len(vars) == 0 {
		return nil, nil
	}
	return json.Marshal(vars)
}
