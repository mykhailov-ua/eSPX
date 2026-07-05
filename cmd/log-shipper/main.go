// Command log-shipper forwards rotated tracker logs to the broker because the hot path must not block on remote I/O.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"espx/pkg/lifecycle"
	"espx/pkg/broker/client"
)

// main runs an async broker fan-out because tracker logging is length-prefixed on disk and must survive broker restarts.
func main() {
	brokerAddr := flag.String("broker", "127.0.0.1:9092", "Broker address")
	redisURL := flag.String("redis-url", "redis://127.0.0.1:6379/0", "Redis URL for leader discovery")
	logFilePath := flag.String("log-file", "/var/log/espx/active.log", "Path to the active log file")
	topic := flag.String("topic", "tracker-logs", "Topic name")
	workersCount := flag.Int("workers", 16, "Number of concurrent workers")
	flag.Parse()

	log.Printf("Starting log shipper targeting broker %s (redis: %s) on topic %s with %d workers", *brokerAddr, *redisURL, *topic, *workersCount)

	ctx, stop := lifecycle.NotifyContext(context.Background())
	defer stop()

	timeouts := lifecycle.TimeoutsFromEnv()
	jobs := make(chan []byte, 10000)
	var wg sync.WaitGroup

	for i := 0; i < *workersCount; i++ {
		wg.Add(1)
		go runWorker(ctx, &wg, workerConfig{
			id:         i,
			brokerAddr: *brokerAddr,
			redisURL:   *redisURL,
			topic:      *topic,
			jobs:       jobs,
		})
	}

	file, err := openLogFile(ctx, *logFilePath)
	if err != nil {
		shutdown(jobs, &wg, timeouts)
		return
	}
	defer file.Close()

	header := make([]byte, 4)
	payloadBuf := make([]byte, 1024*1024)

readLoop:
	for ctx.Err() == nil {
		_, err := io.ReadFull(file, header)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				if next, rotated, reopenErr := tryReopenAfterRotation(ctx, file, *logFilePath); reopenErr != nil {
					break readLoop
				} else if rotated {
					file = next
					continue
				}
				select {
				case <-ctx.Done():
					break readLoop
				case <-time.After(5 * time.Millisecond):
				}
				continue
			}
			log.Printf("Error reading length header: %v", err)
			select {
			case <-ctx.Done():
				break readLoop
			case <-time.After(time.Second):
			}
			continue
		}

		length := binary.BigEndian.Uint32(header)
		if length == 0 {
			continue
		}

		if int(length) > len(payloadBuf) {
			payloadBuf = make([]byte, length)
		}
		if _, err = io.ReadFull(file, payloadBuf[:length]); err != nil {
			log.Printf("Error reading payload: %v", err)
			continue
		}

		payloadCopy := make([]byte, length)
		copy(payloadCopy, payloadBuf[:length])

		select {
		case <-ctx.Done():
			break readLoop
		case jobs <- payloadCopy:
		}
	}

	log.Printf("log shipper shutting down")
	shutdown(jobs, &wg, timeouts)
}

type workerConfig struct {
	id         int
	brokerAddr string
	redisURL   string
	topic      string
	jobs       <-chan []byte
}

func runWorker(ctx context.Context, wg *sync.WaitGroup, cfg workerConfig) {
	defer wg.Done()

	cli := client.NewClient(cfg.brokerAddr, 5*time.Second)
	if cfg.redisURL != "" {
		cli.SetRedisURL(cfg.redisURL)
	}
	for ctx.Err() == nil {
		connectErr := cli.Connect()
		if connectErr == nil {
			break
		}
		log.Printf("[Worker %d] Failed to connect to broker, retrying in 1s: %v", cfg.id, connectErr)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
	if ctx.Err() != nil {
		return
	}
	defer cli.Close()

	var count int64
	lastReport := time.Now()

	for payload := range cfg.jobs {
		_, err := cli.Produce(cfg.topic, 0, payload)
		if err != nil {
			log.Printf("[Worker %d] Error producing: %v", cfg.id, err)
		} else {
			count++
		}

		if time.Since(lastReport) > 5*time.Second {
			log.Printf("[Worker %d] Sent %d messages", cfg.id, count)
			lastReport = time.Now()
		}
	}
}

func shutdown(jobs chan []byte, wg *sync.WaitGroup, timeouts lifecycle.Timeouts) {
	close(jobs)
	if err := lifecycle.Wait(timeouts.Wait, wg.Wait); err != nil {
		log.Printf("log shipper worker drain timed out: %v", err)
	}
	log.Printf("log shipper shutdown complete")
}

func openLogFile(ctx context.Context, path string) (*os.File, error) {
	for ctx.Err() == nil {
		file, err := os.Open(path)
		if err == nil {
			return file, nil
		}
		log.Printf("Waiting for log file %s to be created: %v", path, err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil, ctx.Err()
}

func tryReopenAfterRotation(ctx context.Context, file *os.File, logFilePath string) (*os.File, bool, error) {
	stat, statErr := os.Stat(logFilePath)
	if statErr != nil {
		return file, false, nil
	}
	currStat, fileStatErr := file.Stat()
	if fileStatErr != nil || stat.Size() >= currStat.Size() {
		return file, false, nil
	}

	log.Printf("Log file rotation detected (size shrunk). Reopening.")
	if err := file.Close(); err != nil {
		return nil, false, err
	}
	reopened, err := openLogFile(ctx, logFilePath)
	if err != nil {
		return nil, false, err
	}
	return reopened, true, nil
}
