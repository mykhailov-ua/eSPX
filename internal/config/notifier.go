package config

// NotifierConfigured reports whether at least one delivery channel has credentials in config.
func (c *Config) NotifierConfigured() bool {
	if c == nil {
		return false
	}
	return c.Notifier.TelegramBotToken != "" ||
		c.Notifier.TelegramChatID != "" ||
		c.Notifier.SlackWebhookURL != "" ||
		c.Notifier.SMTPHost != "" ||
		c.Notifier.SMTPSender != ""
}
