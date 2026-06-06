package server

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mykhailov-ua/ad-event-processor/pkg/broker/log"
	"github.com/mykhailov-ua/ad-event-processor/pkg/broker/protocol"
	"github.com/panjf2000/gnet/v2"
)

const MaxConnections int64 = 0

var bytePool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 32)
		return &b
	},
}

type Server struct {
	*gnet.BuiltinEventEngine
	addr          string
	healthAddr    string
	dataDir       string
	maxSegSize    int64
	indexInterval int64
	topics        map[string]*log.PartitionLog
	topicsMu      sync.RWMutex
	closeChan     chan struct{}
	closeOnce     sync.Once
	wg            sync.WaitGroup
	engMu         sync.Mutex
	eng           gnet.Engine
	active        atomic.Bool
	httpSrv       *http.Server
	coord         *Coordinator

	connCount atomic.Int64

	diskOK atomic.Bool
}

func NewServer(addr string, dataDir string, maxSegSize int64, indexInterval int64) *Server {
	s := &Server{
		BuiltinEventEngine: &gnet.BuiltinEventEngine{},
		addr:               addr,
		healthAddr:         "127.0.0.1:0",
		dataDir:            dataDir,
		maxSegSize:         maxSegSize,
		indexInterval:      indexInterval,
		topics:             make(map[string]*log.PartitionLog),
		closeChan:          make(chan struct{}),
	}
	s.diskOK.Store(true)
	return s
}

func (s *Server) SetHealthAddr(addr string) {
	s.healthAddr = addr
}

func (s *Server) SetCoordinator(coord *Coordinator) {
	s.coord = coord
}

func (s *Server) HealthAddr() string {
	return s.healthAddr
}

func (s *Server) Start() error {
	if strings.HasSuffix(s.addr, ":0") {
		l, err := net.Listen("tcp", s.addr)
		if err != nil {
			return err
		}
		s.addr = l.Addr().String()
		_ = l.Close()
	}

	if strings.HasSuffix(s.healthAddr, ":0") {
		l, err := net.Listen("tcp", s.healthAddr)
		if err != nil {
			return err
		}
		s.healthAddr = l.Addr().String()
		_ = l.Close()
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runDiskHealthWorker()
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	s.httpSrv = &http.Server{
		Addr:    s.healthAddr,
		Handler: mux,
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		_ = s.httpSrv.ListenAndServe()
	}()

	s.wg.Add(1)
	errChan := make(chan error, 1)
	go func() {
		defer s.wg.Done()
		addr := "tcp://" + s.addr
		err := gnet.Run(s, addr,
			gnet.WithMulticore(true),
		)
		if err != nil {
			errChan <- err
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-time.After(100 * time.Millisecond):
		return nil
	}
}

func (s *Server) Addr() string {
	return s.addr
}

func (s *Server) runDiskHealthWorker() {
	const interval = 5 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.closeChan:
			return
		case <-ticker.C:
			s.diskOK.Store(s.probeDisk())
		}
	}
}

func (s *Server) probeDisk() bool {
	testFile := filepath.Join(s.dataDir, ".healthcheck")
	f, err := os.OpenFile(testFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		return false
	}
	_ = f.Close()
	_ = os.Remove(testFile)
	return true
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if !s.active.Load() {
		http.Error(w, "server not active", http.StatusServiceUnavailable)
		return
	}

	if !s.diskOK.Load() {
		http.Error(w, "disk not writable", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func (s *Server) Stop() {
	s.closeOnce.Do(func() {
		close(s.closeChan)
		if s.httpSrv != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = s.httpSrv.Shutdown(ctx)
			cancel()
		}
		if s.active.Load() {
			s.engMu.Lock()
			eng := s.eng
			s.engMu.Unlock()
			_ = eng.Stop(context.Background())
		}

		s.topicsMu.Lock()
		for _, pl := range s.topics {
			_ = pl.Close()
		}
		s.topicsMu.Unlock()
	})
	s.wg.Wait()
}

func (s *Server) OnBoot(eng gnet.Engine) gnet.Action {
	s.engMu.Lock()
	s.eng = eng
	s.engMu.Unlock()
	s.active.Store(true)
	s.diskOK.Store(s.probeDisk())
	return gnet.None
}

func (s *Server) OnOpen(c gnet.Conn) ([]byte, gnet.Action) {
	new := s.connCount.Add(1)
	if MaxConnections > 0 && new > MaxConnections {
		s.connCount.Add(-1)
		return nil, gnet.Close
	}
	return nil, gnet.None
}

func (s *Server) OnClose(c gnet.Conn, err error) gnet.Action {
	s.connCount.Add(-1)
	return gnet.None
}

func (s *Server) OnTraffic(c gnet.Conn) gnet.Action {
	for {
		lenBuf, err := c.Peek(4)
		if err != nil {
			return gnet.None
		}
		length := binary.BigEndian.Uint32(lenBuf)

		if length < 10 {
			if _, err := c.Discard(int(4 + length)); err != nil {
				return gnet.Close
			}
			return gnet.Close
		}

		payloadBuf, err := c.Peek(int(4 + length))
		if err != nil {
			return gnet.None
		}

		if _, err := c.Discard(int(4 + length)); err != nil {
			return gnet.Close
		}

		framePayload := payloadBuf[4 : 4+length]
		cmd := binary.BigEndian.Uint16(framePayload[0:2])
		seq := binary.BigEndian.Uint64(framePayload[2:10])
		reqPayload := framePayload[10:]

		switch cmd {
		case protocol.CmdProduce:
			s.handleProduce(c, seq, reqPayload)
		case protocol.CmdFetch:
			s.handleFetch(c, seq, reqPayload)
		default:
			return gnet.Close
		}
	}
}

func (s *Server) handleProduce(c gnet.Conn, seq uint64, payload []byte) {
	bufPtr := bytePool.Get().(*[]byte)
	defer bytePool.Put(bufPtr)
	buf := (*bufPtr)[:32]

	topic, msgPayload, err := protocol.DecodeProduceRequest(payload)
	if err != nil {
		resp := protocol.EncodeProduceResponse(buf, seq, 1, 0)
		_, _ = c.Write(resp)
		return
	}

	if s.coord != nil && !s.coord.IsLeader(topic) {

		resp := protocol.EncodeProduceResponse(buf, seq, 4, 0)
		_, _ = c.Write(resp)
		return
	}

	pl, err := s.getOrCreatePartition(topic)
	if err != nil {
		resp := protocol.EncodeProduceResponse(buf, seq, 2, 0)
		_, _ = c.Write(resp)
		return
	}

	offset, err := pl.Append(msgPayload)
	if err != nil {
		resp := protocol.EncodeProduceResponse(buf, seq, 3, 0)
		_, _ = c.Write(resp)
		return
	}

	resp := protocol.EncodeProduceResponse(buf, seq, 0, offset)
	_, _ = c.Write(resp)
}

func (s *Server) handleFetch(c gnet.Conn, seq uint64, payload []byte) {
	bufPtr := bytePool.Get().(*[]byte)
	defer bytePool.Put(bufPtr)
	buf := (*bufPtr)[:32]

	topic, startOffset, maxBytes, err := protocol.DecodeFetchRequest(payload)
	if err != nil {
		header := protocol.EncodeFetchResponseHeader(buf, seq, 1, 0, 0)
		_, _ = c.Write(header)
		return
	}

	pl, err := s.getOrCreatePartition(topic)
	if err != nil {
		header := protocol.EncodeFetchResponseHeader(buf, seq, 2, 0, 0)
		_, _ = c.Write(header)
		return
	}

	data, err := pl.ReadRawMessages(startOffset, maxBytes)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, log.ErrSegmentNotFound) {
			header := protocol.EncodeFetchResponseHeader(buf, seq, 0, 0, 0)
			_, _ = c.Write(header)
			return
		}
		header := protocol.EncodeFetchResponseHeader(buf, seq, 3, 0, 0)
		_, _ = c.Write(header)
		return
	}

	msgCount, totalBytes := countMessages(data)

	header := protocol.EncodeFetchResponseHeader(buf, seq, 0, msgCount, totalBytes)
	_, _ = c.Write(header)
	if totalBytes > 0 {
		_, _ = c.Write(data)
	}
}

func countMessages(buf []byte) (uint32, uint32) {
	var count, total uint32
	pos := 0
	for pos+12 <= len(buf) {
		length := binary.BigEndian.Uint32(buf[pos : pos+4])
		recordLen := int(12 + int(length) - 8)
		if pos+recordLen > len(buf) {
			break
		}
		count++
		total += uint32(recordLen)
		pos += recordLen
	}
	return count, total
}

func (s *Server) getOrCreatePartition(topic string) (*log.PartitionLog, error) {
	s.topicsMu.RLock()
	pl, ok := s.topics[topic]
	s.topicsMu.RUnlock()
	if ok {
		return pl, nil
	}

	s.topicsMu.Lock()
	defer s.topicsMu.Unlock()
	pl, ok = s.topics[topic]
	if ok {
		return pl, nil
	}

	dir := filepath.Join(s.dataDir, topic)
	var err error
	pl, err = log.NewPartitionLog(dir, s.maxSegSize, s.indexInterval)
	if err != nil {
		return nil, err
	}
	s.topics[topic] = pl
	return pl, nil
}
