package notifier

import (
	"context"
	"fmt"
	"log/slog"
	"net/smtp"
)

// SMTPProvider delivers HTML email via plain SMTP with optional AUTH.
type SMTPProvider struct {
	host     string
	port     string
	username string
	password string
	sender   string
	breaker  *CircuitBreaker
}

// NewSMTPProvider binds SMTP credentials; port defaults to 587 when empty.
func NewSMTPProvider(host, port, username, password, sender string, breaker *CircuitBreaker) *SMTPProvider {
	if port == "" {
		port = "587"
	}
	return &SMTPProvider{
		host:     host,
		port:     port,
		username: username,
		password: password,
		sender:   sender,
		breaker:  breaker,
	}
}

func (s *SMTPProvider) Name() string {
	return "SMTP"
}

// Send delivers one message; cancel returns when ctx ends but SendMail may still run in the background.
func (s *SMTPProvider) Send(ctx context.Context, recipient, title, body string) error {
	if !s.breaker.Allow() {
		return ErrCircuitOpen
	}

	if recipient == "" {
		slog.Warn("smtp notification skipped: recipient is required")
		return fmt.Errorf("smtp recipient is required")
	}

	if s.host == "" || s.sender == "" {
		slog.Info("smtp notification dry-run", "to", recipient, "title", title, "body", body)
		return nil
	}

	subject := title
	if subject == "" {
		subject = "Notification Alert"
	}

	msg := []byte(fmt.Sprintf("To: %s\r\n"+
		"From: %s\r\n"+
		"Subject: %s\r\n"+
		"Content-Type: text/html; charset=UTF-8\r\n\r\n"+
		"%s\r\n", recipient, s.sender, subject, body))

	addr := fmt.Sprintf("%s:%s", s.host, s.port)
	var auth smtp.Auth
	if s.username != "" {
		auth = smtp.PlainAuth("", s.username, s.password, s.host)
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- smtp.SendMail(addr, auth, s.sender, []string{recipient}, msg)
	}()

	select {
	case <-ctx.Done():
		s.breaker.RecordFailure()
		return ctx.Err()
	case err := <-errChan:
		if err != nil {
			s.breaker.RecordFailure()
			return err
		}
	}

	s.breaker.RecordSuccess()
	return nil
}
