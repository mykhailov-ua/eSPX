package notifier

import (
	"regexp"
	"strings"
	"sync"
)

var (
	adminBaseURLMu sync.RWMutex
	adminBaseURL   = "https://admin.espx.dev"
	ipAddressRegex = regexp.MustCompile(`\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b`)
)

// SetAdminBaseURL configures acknowledge/blacklist links for interactive provider buttons.
func SetAdminBaseURL(baseURL string) {
	adminBaseURLMu.Lock()
	defer adminBaseURLMu.Unlock()
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		adminBaseURL = "https://admin.espx.dev"
		return
	}
	adminBaseURL = baseURL
}

func currentAdminBaseURL() string {
	adminBaseURLMu.RLock()
	defer adminBaseURLMu.RUnlock()
	return adminBaseURL
}

// InteractiveActions holds optional admin deep-links derived from notification content.
type InteractiveActions struct {
	AcknowledgeURL string
	BlockIPURL     string
	BlockIP        string
}

// BuildInteractiveActions extracts admin URLs for acknowledge and IP block buttons.
func BuildInteractiveActions(notificationID, title, body string) InteractiveActions {
	base := currentAdminBaseURL()
	var actions InteractiveActions

	if notificationID != "" {
		actions.AcknowledgeURL = base + "/admin/acknowledge?id=" + notificationID
	}
	if ip := ipAddressRegex.FindString(body + " " + title); ip != "" {
		actions.BlockIP = ip
		actions.BlockIPURL = base + "/admin/blacklist?ip=" + ip + "&source=manual"
	}
	return actions
}
