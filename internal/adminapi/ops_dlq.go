package adminapi

// DLQRetryPayload is the outbox command body for re-queuing a DLQ entry.
type DLQRetryPayload struct {
	ShardID int    `json:"shard_id"`
	Stream  string `json:"stream"`
	EntryID string `json:"entry_id"`
	DLQID   string `json:"dlq_id"`
}
