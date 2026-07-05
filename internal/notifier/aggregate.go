package notifier

import (
	"fmt"
	"strings"
	"time"

	"espx/internal/notifier/db"
)

func buildAggregatedBody(items []db.NotifierNotification) string {
	if len(items) <= 1 {
		return items[0].Body
	}

	lead := items[0]
	var sb strings.Builder
	sb.WriteString(lead.Body)
	sb.WriteString("\n\n---\n")
	sb.WriteString(fmt.Sprintf("⚠️ [DEDUPLICATED] Accumulated %d similar events.\n", len(items)))

	firstTime := lead.CreatedAt.Time
	lastTime := lead.CreatedAt.Time
	for _, item := range items {
		if item.CreatedAt.Time.Before(firstTime) {
			firstTime = item.CreatedAt.Time
		}
		if item.CreatedAt.Time.After(lastTime) {
			lastTime = item.CreatedAt.Time
		}
	}
	sb.WriteString(fmt.Sprintf("First seen: %s\n", firstTime.Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("Last seen: %s\n", lastTime.Format(time.RFC3339)))

	uniqueBodies := make(map[string]bool)
	for _, item := range items {
		uniqueBodies[item.Body] = true
	}
	if len(uniqueBodies) > 1 {
		sb.WriteString("\nUnique event details:\n")
		detailsCount := 0
		for body := range uniqueBodies {
			if body != lead.Body {
				sb.WriteString(fmt.Sprintf("- %s\n", body))
				detailsCount++
				if detailsCount >= 5 {
					sb.WriteString("- ... (truncated)\n")
					break
				}
			}
		}
	}
	return sb.String()
}
