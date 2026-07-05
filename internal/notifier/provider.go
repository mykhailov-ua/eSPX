package notifier

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Provider abstracts outbound delivery to Telegram, Slack, or SMTP.
type Provider interface {
	Send(ctx context.Context, recipient, title, body string) error
	Name() string
}

// MockSentNotification records one mock delivery for integration tests.
type MockSentNotification struct {
	Recipient string
	Title     string
	Body      string
	SentAt    time.Time
}

// MockProvider captures sent messages in-process for integration tests.
type MockProvider struct {
	breaker      *CircuitBreaker
	ProviderName string
	ShouldFail   bool
	Sent         []MockSentNotification
}

// NewMockProvider returns a provider that records deliveries without network calls.
func NewMockProvider(breaker *CircuitBreaker) *MockProvider {
	return &MockProvider{breaker: breaker}
}

func (m *MockProvider) Name() string {
	if m.ProviderName != "" {
		return m.ProviderName
	}
	return "TELEGRAM"
}

// Send records the message; body containing trigger_failure simulates a provider error.
func (m *MockProvider) Send(ctx context.Context, recipient, title, body string) error {
	_ = ctx
	if !m.breaker.Allow() {
		return ErrCircuitOpen
	}

	if strings.Contains(body, "trigger_failure") || m.ShouldFail {
		m.breaker.RecordFailure()
		return fmt.Errorf("mock send failure triggered")
	}

	m.Sent = append(m.Sent, MockSentNotification{
		Recipient: recipient,
		Title:     title,
		Body:      body,
		SentAt:    time.Now(),
	})
	m.breaker.RecordSuccess()
	return nil
}
