// Package client is a TCP broker client with leader redirect via Redis coordination.
package client

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"espx/pkg/broker/protocol"
	"github.com/redis/go-redis/v9"
)

// MessageIterator walks fetch response bytes without copying individual payloads.
type MessageIterator struct {
	data          []byte
	idx           int
	count         uint32
	curr          uint32
	Offset        uint64
	Payload       []byte
	HighWatermark uint64
}

// Next advances the iterator across one fetched log record.
func (it *MessageIterator) Next() bool {
	if it.curr >= it.count || it.idx+12 > len(it.data) {
		return false
	}
	length := binary.BigEndian.Uint32(it.data[it.idx : it.idx+4])
	it.Offset = binary.BigEndian.Uint64(it.data[it.idx+4 : it.idx+12])
	payloadLen := int(length) - 8
	if it.idx+12+payloadLen > len(it.data) {
		return false
	}
	it.Payload = it.data[it.idx+12 : it.idx+12+payloadLen]
	it.idx += 12 + payloadLen
	it.curr++
	return true
}

// Client is a framed TCP broker client with optional Redis-backed leader discovery.
type Client struct {
	addr      string
	conn      *net.TCPConn
	mu        sync.Mutex
	nextSeq   uint64
	readBuf   []byte
	writeBuf  []byte
	lenBuf    []byte
	timeout   time.Duration
	redisURL  string
	rdb       *redis.Client
	fetchIter MessageIterator
}

// NewClient allocates reusable read and write buffers for the broker wire protocol.
func NewClient(addr string, timeout time.Duration) *Client {
	return &Client{
		addr:     addr,
		timeout:  timeout,
		readBuf:  make([]byte, 1024*1024),
		writeBuf: make([]byte, 1024*1024),
		lenBuf:   make([]byte, 4),
	}
}

// SetRedisURL enables leader lookup so Produce and Fetch survive broker failover.
func (c *Client) SetRedisURL(url string) {
	c.redisURL = url
}

// Connect dials the broker if the client is not already connected.
func (c *Client) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connectLocked()
}

// connectLocked opens the TCP session while the caller already holds the client mutex.
func (c *Client) connectLocked() error {
	if c.conn != nil {
		return nil
	}

	conn, err := net.DialTimeout("tcp", c.addr, c.timeout)
	if err != nil {
		return err
	}
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		_ = conn.Close()
		return errors.New("broker client requires tcp")
	}
	c.conn = tc
	return nil
}

// Close tears down the TCP and Redis handles held by the client.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error
	if c.conn != nil {
		err = c.conn.Close()
		c.conn = nil
	}
	if c.rdb != nil {
		_ = c.rdb.Close()
		c.rdb = nil
	}
	return err
}

// getConn returns a live connection, dialing lazily when needed.
func (c *Client) getConn() (*net.TCPConn, error) {
	if c.conn == nil {
		if err := c.connectLocked(); err != nil {
			return nil, err
		}
	}
	return c.conn, nil
}

// Produce retries across leader failover; callers must not pin to a stale broker address.
func (c *Client) Produce(topic string, partition uint16, payload []byte) (uint64, error) {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			time.Sleep(500 * time.Millisecond)
		}

		c.mu.Lock()
		conn, err := c.getConn()
		if err != nil {
			c.mu.Unlock()
			lastErr = err

			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic, partition); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		seq := atomic.AddUint64(&c.nextSeq, 1)
		req := protocol.EncodeProduceRequest(c.writeBuf, seq, topic, partition, payload)

		if c.timeout > 0 {
			_ = conn.SetDeadline(time.Now().Add(c.timeout))
		}

		if _, err := conn.Write(req); err != nil {
			_ = c.closeRawConn()
			c.mu.Unlock()
			lastErr = err
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic, partition); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		cmd, respSeq, respPayload, err := protocol.ReadFrameTCP(conn, c.readBuf, c.lenBuf)
		if err != nil {
			_ = c.closeRawConn()
			c.mu.Unlock()
			lastErr = err
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic, partition); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		if cmd != protocol.CmdProduceResp {
			c.mu.Unlock()
			return 0, fmt.Errorf("unexpected command response: %d", cmd)
		}

		if respSeq != seq {
			c.mu.Unlock()
			return 0, fmt.Errorf("sequence mismatch: expected %d, got %d", seq, respSeq)
		}

		if len(respPayload) < 9 {
			c.mu.Unlock()
			return 0, errors.New("malformed produce response payload")
		}

		status := respPayload[0]
		if status == 4 || status == 5 || status == 6 || status == 7 {
			_ = c.closeRawConn()
			c.mu.Unlock()
			switch status {
			case 5:
				lastErr = errors.New("stale fencing epoch")
			case 6:
				lastErr = errors.New("leader catching up")
			case 7:
				lastErr = errors.New("broker overloaded")
			default:
				lastErr = errors.New("not leader")
			}
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic, partition); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		if status != 0 {
			c.mu.Unlock()
			return 0, fmt.Errorf("broker error status: %d", status)
		}

		offsetVal := binary.BigEndian.Uint64(respPayload[1:9])
		c.mu.Unlock()
		return offsetVal, nil
	}

	return 0, fmt.Errorf("failed after 5 attempts, last error: %w", lastErr)
}

// Fetch follows the same redirect policy as Produce for HA follower reads.
// The returned iterator aliases client-owned buffers and is valid until the next Fetch.
func (c *Client) Fetch(topic string, partition uint16, startOffset uint64, maxBytes uint32) (*MessageIterator, error) {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			time.Sleep(500 * time.Millisecond)
		}

		c.mu.Lock()
		conn, err := c.getConn()
		if err != nil {
			c.mu.Unlock()
			lastErr = err
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic, partition); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		seq := atomic.AddUint64(&c.nextSeq, 1)
		req := protocol.EncodeFetchRequest(c.writeBuf, seq, topic, partition, startOffset, maxBytes)

		if c.timeout > 0 {
			_ = conn.SetDeadline(time.Now().Add(c.timeout))
		}

		if _, err := conn.Write(req); err != nil {
			_ = c.closeRawConn()
			c.mu.Unlock()
			lastErr = err
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic, partition); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		cmd, respSeq, respPayload, err := protocol.ReadFrameTCP(conn, c.readBuf, c.lenBuf)
		if err != nil {
			_ = c.closeRawConn()
			c.mu.Unlock()
			lastErr = err
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic, partition); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		if cmd != protocol.CmdFetchResp {
			c.mu.Unlock()
			return nil, fmt.Errorf("unexpected command response: %d", cmd)
		}

		if respSeq != seq {
			c.mu.Unlock()
			return nil, fmt.Errorf("sequence mismatch: expected %d, got %d", seq, respSeq)
		}

		if len(respPayload) < protocol.FetchRespMetaLen {
			c.mu.Unlock()
			return nil, errors.New("malformed fetch response payload")
		}

		status := respPayload[0]
		if status == 4 || status == 5 || status == 6 || status == 7 {
			_ = c.closeRawConn()
			c.mu.Unlock()
			switch status {
			case 5:
				lastErr = errors.New("stale fencing epoch")
			case 6:
				lastErr = errors.New("leader catching up")
			case 7:
				lastErr = errors.New("broker overloaded")
			default:
				lastErr = errors.New("not leader")
			}
			if c.redisURL != "" {
				if newAddr, rErr := c.resolveLeaderAddr(topic, partition); rErr == nil && newAddr != c.addr {
					c.addr = newAddr
				}
			}
			continue
		}

		if status != 0 {
			c.mu.Unlock()
			return nil, fmt.Errorf("broker error status: %d", status)
		}

		msgCount := binary.BigEndian.Uint32(respPayload[1:5])
		highWatermark := binary.BigEndian.Uint64(respPayload[5:13])
		messagesData := respPayload[protocol.FetchRespMetaLen:]

		c.fetchIter.data = messagesData
		c.fetchIter.idx = 0
		c.fetchIter.count = msgCount
		c.fetchIter.curr = 0
		c.fetchIter.Offset = 0
		c.fetchIter.Payload = nil
		c.fetchIter.HighWatermark = highWatermark

		c.mu.Unlock()
		return &c.fetchIter, nil
	}

	return nil, fmt.Errorf("failed after 5 attempts, last error: %w", lastErr)
}

// CommitOffset persists the next fetch offset for a consumer group partition.
func (c *Client) CommitOffset(topic string, partition uint16, group string, offset uint64) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, err := c.getConn()
	if err != nil {
		return 0, err
	}

	seq := atomic.AddUint64(&c.nextSeq, 1)
	req := protocol.EncodeCommitOffsetRequest(c.writeBuf, seq, topic, partition, group, offset)

	if c.timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(c.timeout))
	}
	if _, err := conn.Write(req); err != nil {
		_ = c.closeRawConn()
		return 0, err
	}

	cmd, respSeq, respPayload, err := protocol.ReadFrameTCP(conn, c.readBuf, c.lenBuf)
	if err != nil {
		_ = c.closeRawConn()
		return 0, err
	}
	if cmd != protocol.CmdCommitOffsetResp {
		return 0, fmt.Errorf("unexpected command response: %d", cmd)
	}
	if respSeq != seq {
		return 0, fmt.Errorf("sequence mismatch: expected %d, got %d", seq, respSeq)
	}

	status, stored, err := protocol.DecodeCommitOffsetResponse(respPayload)
	if err != nil {
		return 0, err
	}
	if status != 0 {
		return 0, fmt.Errorf("broker commit offset status: %d", status)
	}
	return stored, nil
}

// CommittedOffset returns the stored next-fetch offset for a consumer group partition.
func (c *Client) CommittedOffset(topic string, partition uint16, group string) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, err := c.getConn()
	if err != nil {
		return 0, err
	}

	seq := atomic.AddUint64(&c.nextSeq, 1)
	req := protocol.EncodeCommittedOffsetRequest(c.writeBuf, seq, topic, partition, group)

	if c.timeout > 0 {
		_ = conn.SetDeadline(time.Now().Add(c.timeout))
	}
	if _, err := conn.Write(req); err != nil {
		_ = c.closeRawConn()
		return 0, err
	}

	cmd, respSeq, respPayload, err := protocol.ReadFrameTCP(conn, c.readBuf, c.lenBuf)
	if err != nil {
		_ = c.closeRawConn()
		return 0, err
	}
	if cmd != protocol.CmdCommittedOffsetResp {
		return 0, fmt.Errorf("unexpected command response: %d", cmd)
	}
	if respSeq != seq {
		return 0, fmt.Errorf("sequence mismatch: expected %d, got %d", seq, respSeq)
	}

	status, offset, err := protocol.DecodeCommittedOffsetResponse(respPayload)
	if err != nil {
		return 0, err
	}
	if status != 0 {
		return 0, fmt.Errorf("broker committed offset status: %d", status)
	}
	return offset, nil
}

// closeRawConn drops a broken socket so the next RPC establishes a fresh session.
func (c *Client) closeRawConn() error {
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

// resolveLeaderAddr maps a topic partition to the current leader broker address via Redis coordination keys.
func (c *Client) resolveLeaderAddr(topic string, partition uint16) (string, error) {
	if c.redisURL == "" {
		return "", errors.New("redis URL not set")
	}
	if c.rdb == nil {
		opts, err := redis.ParseURL(c.redisURL)
		if err != nil {
			return "", err
		}
		c.rdb = redis.NewClient(opts)
	}
	tpKey := protocol.TopicPartitionID(topic, partition)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	leaderID, err := c.rdb.Get(ctx, "espx:topics:"+tpKey+":leader").Result()
	if err != nil {
		return "", err
	}
	return c.rdb.Get(ctx, "espx:brokers:"+leaderID).Result()
}
