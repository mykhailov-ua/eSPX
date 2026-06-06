package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mykhailov-ua/ad-event-processor/pkg/broker/client"
	"github.com/redis/go-redis/v9"
)

type Coordinator struct {
	nodeID    string
	tcpAddr   string
	rdb       redis.UniversalClient
	server    *Server
	closeChan chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
	leaders   map[string]bool
	leadersMu sync.RWMutex
}

func NewCoordinator(nodeID string, tcpAddr string, redisURL string, server *Server) (*Coordinator, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis URL: %w", err)
	}

	rdb := redis.NewClient(opts)
	return &Coordinator{
		nodeID:    nodeID,
		tcpAddr:   tcpAddr,
		rdb:       rdb,
		server:    server,
		closeChan: make(chan struct{}),
		leaders:   make(map[string]bool),
	}, nil
}

func (c *Coordinator) Start() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runHeartbeatLoop()
	}()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runCoordinationLoop()
	}()
}

func (c *Coordinator) Stop() {
	c.closeOnce.Do(func() {
		close(c.closeChan)
	})
	c.wg.Wait()
	_ = c.rdb.Close()
}

func (c *Coordinator) IsLeader(topic string) bool {
	c.leadersMu.RLock()
	defer c.leadersMu.RUnlock()
	return c.leaders[topic]
}

func (c *Coordinator) runHeartbeatLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeChan:
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = c.rdb.Del(ctx, "espx:brokers:"+c.nodeID).Err()
			cancel()
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = c.rdb.Set(ctx, "espx:brokers:"+c.nodeID, c.tcpAddr, 5*time.Second).Err()
			cancel()
		}
	}
}

func (c *Coordinator) runCoordinationLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	replications := make(map[string]chan struct{})

	for {
		select {
		case <-c.closeChan:
			for _, stopCh := range replications {
				close(stopCh)
			}
			return
		case <-ticker.C:
			c.server.topicsMu.RLock()
			topics := make([]string, 0, len(c.server.topics))
			for t := range c.server.topics {
				topics = append(topics, t)
			}
			c.server.topicsMu.RUnlock()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			for _, topic := range topics {
				leaderKey := "espx:topics:" + topic + ":leader"

				ok, err := c.rdb.SetNX(ctx, leaderKey, c.nodeID, 5*time.Second).Result()
				if err != nil {
					continue
				}

				if ok {
					c.setLeaderStatus(topic, true)
					if stopCh, exists := replications[topic]; exists {
						close(stopCh)
						delete(replications, topic)
					}
					_ = c.rdb.Expire(ctx, leaderKey, 5*time.Second).Err()
				} else {
					currentLeader, err := c.rdb.Get(ctx, leaderKey).Result()
					if err == nil && currentLeader == c.nodeID {
						c.setLeaderStatus(topic, true)
						_ = c.rdb.Expire(ctx, leaderKey, 5*time.Second).Err()
					} else {
						if _, exists := replications[topic]; !exists {
							stopCh := make(chan struct{})
							replications[topic] = stopCh
							c.wg.Add(1)
							go func(t string, sCh chan struct{}) {
								defer c.wg.Done()
								c.replicate(t, currentLeader, sCh)
							}(topic, stopCh)
						}
					}
				}
			}
			cancel()
		}
	}
}

func (c *Coordinator) setLeaderStatus(topic string, isLeader bool) {
	c.leadersMu.Lock()
	c.leaders[topic] = isLeader
	c.leadersMu.Unlock()
}

func (c *Coordinator) replicate(topic string, leaderID string, stopCh chan struct{}) {
	slog.Info("Starting replication", "topic", topic, "leader", leaderID)
	defer slog.Info("Stopped replication", "topic", topic)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var cli *client.Client
	var currentAddr string

	defer func() {
		if cli != nil {
			_ = cli.Close()
		}
	}()

	for {
		select {
		case <-stopCh:
			return
		case <-c.closeChan:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			leaderAddr, err := c.rdb.Get(ctx, "espx:brokers:"+leaderID).Result()
			cancel()
			if err != nil {
				if cli != nil {
					_ = cli.Close()
					cli = nil
				}
				continue
			}

			if cli == nil || leaderAddr != currentAddr {
				if cli != nil {
					_ = cli.Close()
				}
				cli = client.NewClient(leaderAddr, time.Second)
				currentAddr = leaderAddr
				if err := cli.Connect(); err != nil {
					_ = cli.Close()
					cli = nil
					continue
				}
			}

			pl, err := c.server.getOrCreatePartition(topic)
			if err != nil {
				continue
			}

			nextOffset := pl.NextOffset()

			err = cli.FetchStream(topic, nextOffset, 65536, func(offset uint64, payload []byte) error {
				_, err := pl.Append(payload)
				return err
			})

			if err != nil {
				_ = cli.Close()
				cli = nil
				if !errors.Is(err, errors.New("EOF")) {
					time.Sleep(500 * time.Millisecond)
				}
			}
		}
	}
}
