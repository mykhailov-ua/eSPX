// Command log-shipper forwards rotated tracker logs to the broker because the hot path must not block on remote I/O.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"espx/internal/ads/pb"
	"espx/internal/config"
	"espx/pkg/broker/client"
	"espx/pkg/broker/partition"
)

type shipJob struct {
	partition uint16
	payload   []byte
}

// main runs an async broker fan-out because tracker logging is length-prefixed on disk and must survive broker restarts.
func main() {
	brokerAddr := flag.String("broker", "127.0.0.1:9092", "Broker address")
	redisURL := flag.String("redis-url", "redis://127.0.0.1:6379/0", "Redis URL for leader discovery")
	logFilePath := flag.String("log-file", "/var/log/espx/active.log", "Path to the active log file")
	topic := flag.String("topic", "tracker-logs", "Topic name")
	partitions := flag.Int("partitions", 6, "Topic partition count (matches Redis shard count)")
	workersCount := flag.Int("workers", 16, "Number of concurrent workers")
	flag.Parse()

	log.Printf("Starting log shipper targeting broker %s (redis: %s) on topic %s (%d partitions) with %d workers",
		*brokerAddr, *redisURL, *topic, *partitions, *workersCount)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	jobs := make(chan shipJob, 10000)
	var wg sync.WaitGroup

	for i := 0; i < *workersCount; i++ {
		wg.Add(1)
		go runShipperWorker(ctx, &wg, i, *brokerAddr, *redisURL, *topic, jobs)
	}

	go tailLogFile(ctx, *logFilePath, *partitions, jobs)

	<-ctx.Done()
	log.Printf("shutting down log-shipper, draining %d workers", *workersCount)

	drained := make(chan struct{})
	go func() {
		wg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		log.Printf("log-shipper shutdown complete")
	case <-time.After(config.LifecycleShutdownTimeout()):
		log.Printf("log-shipper drain timed out after %s", config.LifecycleShutdownTimeout())
	}
}

// runShipperWorker connects to the broker and produces payloads until jobs is closed or ctx is cancelled.
func runShipperWorker(ctx context.Context, wg *sync.WaitGroup, workerID int, brokerAddr, redisURL, topic string, jobs <-chan shipJob) {
	defer wg.Done()

	cli := client.NewClient(brokerAddr, 5*time.Second)
	if redisURL != "" {
		cli.SetRedisURL(redisURL)
	}
	for {
		if ctx.Err() != nil {
			return
		}
		err := cli.Connect()
		if err == nil {
			break
		}
		log.Printf("[Worker %d] Failed to connect to broker, retrying in 1s: %v", workerID, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
	defer cli.Close()

	var count int64
	lastReport := time.Now()

	for job := range jobs {
		if ctx.Err() != nil {
			return
		}
		_, err := cli.Produce(topic, job.partition, job.payload)
		if err != nil {
			log.Printf("[Worker %d] Error producing partition %d: %v", workerID, job.partition, err)
		} else {
			count++
		}

		if time.Since(lastReport) > 5*time.Second {
			log.Printf("[Worker %d] Sent %d messages", workerID, count)
			lastReport = time.Now()
		}
	}
}

// routePartition picks the broker partition from an AdStreamEvent campaign_id.
func routePartition(payload []byte, numPartitions int) uint16 {
	if numPartitions <= 1 {
		return 0
	}
	evt := &pb.AdStreamEvent{}
	if err := evt.UnmarshalVT(payload); err != nil {
		return 0
	}
	if len(evt.CampaignId) < 16 {
		return 0
	}
	return partition.Index(evt.CampaignId, numPartitions)
}

// tailLogFile reads length-prefixed records from the active log and enqueues them for broker workers.
func tailLogFile(ctx context.Context, logFilePath string, numPartitions int, jobs chan<- shipJob) {
	defer close(jobs)

	var file *os.File
	var err error
	for {
		if ctx.Err() != nil {
			return
		}
		file, err = os.Open(logFilePath)
		if err == nil {
			break
		}
		log.Printf("Waiting for log file %s to be created: %v", logFilePath, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
	defer file.Close()

	header := make([]byte, 4)
	payloadBuf := make([]byte, 1024*1024)

	for {
		if ctx.Err() != nil {
			return
		}

		_, err := io.ReadFull(file, header)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				stat, statErr := os.Stat(logFilePath)
				if statErr == nil {
					currStat, fileStatErr := file.Stat()
					if fileStatErr == nil && stat.Size() < currStat.Size() {
						log.Printf("Log file rotation detected (size shrunk). Reopening.")
						file.Close()
						for {
							if ctx.Err() != nil {
								return
							}
							file, err = os.Open(logFilePath)
							if err == nil {
								break
							}
							select {
							case <-ctx.Done():
								return
							case <-time.After(100 * time.Millisecond):
							}
						}
						continue
					}
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Millisecond):
					continue
				}
			}
			log.Printf("Error reading length header: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				continue
			}
		}

		length := binary.BigEndian.Uint32(header)
		if length == 0 {
			continue
		}

		if int(length) > len(payloadBuf) {
			payloadBuf = make([]byte, length)
		}
		_, err = io.ReadFull(file, payloadBuf[:length])
		if err != nil {
			log.Printf("Error reading payload: %v", err)
			continue
		}

		payloadCopy := make([]byte, length)
		copy(payloadCopy, payloadBuf[:length])
		part := routePartition(payloadCopy, numPartitions)

		select {
		case <-ctx.Done():
			return
		case jobs <- shipJob{partition: part, payload: payloadCopy}:
		}
	}
}
