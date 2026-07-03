// Package consumer reads broker topics with at-least-once delivery and offset commits.
package consumer

import (
	"context"
	"errors"
	"time"

	"espx/pkg/broker/client"
)

// Handler processes one log record; returning an error stops the consumer without committing.
type Handler func(payload []byte, offset uint64) error

// Config wires a broker consumer group to a topic partition.
type Config struct {
	BrokerAddr string
	RedisURL   string
	Topic      string
	Partition  uint16
	Group      string
	MaxBytes   uint32
	Timeout    time.Duration
	IdleWait   time.Duration
}

// Consumer fetches broker records and commits offsets after successful handler batches.
type Consumer struct {
	cfg     Config
	handler Handler
	cli     *client.Client
}

// New builds a consumer with sane defaults for cold-path stream processing.
func New(cfg Config, handler Handler) *Consumer {
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = 1024 * 1024
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.IdleWait == 0 {
		cfg.IdleWait = 250 * time.Millisecond
	}
	return &Consumer{
		cfg:     cfg,
		handler: handler,
		cli:     client.NewClient(cfg.BrokerAddr, cfg.Timeout),
	}
}

// Run consumes until ctx is cancelled, committing offsets after each successful fetch batch.
func (c *Consumer) Run(ctx context.Context) error {
	if c.handler == nil {
		return errors.New("consumer handler is nil")
	}
	if c.cfg.Topic == "" || c.cfg.Group == "" {
		return errors.New("consumer topic and group are required")
	}
	if c.cfg.RedisURL != "" {
		c.cli.SetRedisURL(c.cfg.RedisURL)
	}
	if err := c.cli.Connect(); err != nil {
		return err
	}
	defer func() { _ = c.cli.Close() }()

	start, err := c.cli.CommittedOffset(c.cfg.Topic, c.cfg.Partition, c.cfg.Group)
	if err != nil {
		return err
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		iter, err := c.cli.Fetch(c.cfg.Topic, c.cfg.Partition, start, c.cfg.MaxBytes)
		if err != nil {
			if c.cfg.RedisURL != "" {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(c.cfg.IdleWait):
				}
				continue
			}
			return err
		}

		var nextCommit uint64
		processed := 0
		for iter.Next() {
			if ctx.Err() != nil {
				break
			}
			off := iter.Offset
			if err := c.handler(iter.Payload, off); err != nil {
				if nextCommit > 0 {
					if _, commitErr := c.cli.CommitOffset(c.cfg.Topic, c.cfg.Partition, c.cfg.Group, nextCommit); commitErr != nil {
						return commitErr
					}
				}
				return err
			}
			nextCommit = off + 1
			processed++
		}

		if nextCommit > 0 {
			stored, err := c.cli.CommitOffset(c.cfg.Topic, c.cfg.Partition, c.cfg.Group, nextCommit)
			if err != nil {
				if c.cfg.RedisURL != "" {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(c.cfg.IdleWait):
					}
					continue
				}
				return err
			}
			start = stored
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if processed == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.cfg.IdleWait):
			}
			continue
		}

		start = nextCommit
	}
}
