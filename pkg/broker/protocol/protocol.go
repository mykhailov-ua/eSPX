// Package protocol defines the internal broker wire format shared by client and server.
package protocol

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

const (
	CmdProduce           uint16 = 1
	CmdFetch             uint16 = 2
	CmdProduceBatch      uint16 = 3
	CmdRegisterTopic     uint16 = 4
	CmdProduceResp       uint16 = 101
	CmdFetchResp         uint16 = 102
	CmdProduceBatchResp  uint16 = 103
	CmdRegisterTopicResp uint16 = 104
)

// FetchRespMetaLen is the fetch response prefix: status + msgCount + highWatermark.
const FetchRespMetaLen = 13

// ProduceBatchRespMetaLen is status + lastOffset + committedCount in a batch produce reply.
const ProduceBatchRespMetaLen = 13

// TopicMetadata binds a compact topic ID to its name for batch decode on the wire.
type TopicMetadata struct {
	ID   uint16
	Name string
}

// TopicRegistry assigns compact numeric IDs so batch produce frames stay small on the wire.
type TopicRegistry struct {
	mu         sync.Mutex
	topics     [65536]unsafe.Pointer
	byName     map[string]uint16
	nextID     uint32
	fileStore  *FileRegistryStore
	redisStore topicRedisRegistrar
}

// topicRedisRegistrar allocates topic IDs in Redis for HA clusters.
type topicRedisRegistrar interface {
	Register(ctx context.Context, name string) (uint16, error)
}

// NewTopicRegistry creates the ID table producers and servers share for batch framing.
func NewTopicRegistry() *TopicRegistry {
	return &TopicRegistry{
		byName: make(map[string]uint16),
	}
}

// SetFileStore enables on-disk persistence for topic IDs across broker restarts.
func (r *TopicRegistry) SetFileStore(store *FileRegistryStore) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fileStore = store
}

// SetRedisStore enables cluster-wide topic ID allocation via Redis.
func (r *TopicRegistry) SetRedisStore(store topicRedisRegistrar) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.redisStore = store
}

// Load restores topic IDs from the configured file store.
func (r *TopicRegistry) Load() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fileStore == nil {
		return nil
	}
	snap, err := r.fileStore.Load()
	if err != nil {
		return err
	}
	return r.applySnapshotLocked(snap, false)
}

// Merge applies an external snapshot; Redis/cluster entries win on name conflicts.
func (r *TopicRegistry) Merge(snap RegistrySnapshot) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.applySnapshotLocked(snap, true)
}

func (r *TopicRegistry) applySnapshotLocked(snap RegistrySnapshot, clusterWins bool) error {
	for name, id := range snap.Topics {
		if err := validateTopicName(name); err != nil {
			continue
		}
		if err := validateTopicID(id); err != nil {
			continue
		}
		if existing, ok := r.byName[name]; ok && existing != id {
			if !clusterWins {
				return fmt.Errorf("topic %q id conflict: local %d vs snapshot %d", name, existing, id)
			}
		}
		if err := r.installLocked(name, id); err != nil {
			return err
		}
	}
	r.rebuildNextIDLocked(snap.NextID)
	return r.persistLocked()
}

func (r *TopicRegistry) rebuildNextIDLocked(snapshotNext uint32) {
	var maxID uint16
	for _, id := range r.byName {
		if id > maxID {
			maxID = id
		}
	}
	next := uint32(maxID)
	if snapshotNext > next {
		next = snapshotNext
	}
	atomic.StoreUint32(&r.nextID, next)
}

func (r *TopicRegistry) installLocked(name string, id uint16) error {
	if err := validateTopicName(name); err != nil {
		return err
	}
	if err := validateTopicID(id); err != nil {
		return err
	}
	meta := &TopicMetadata{
		ID:   id,
		Name: strings.Clone(name),
	}
	r.byName[name] = id
	atomic.StorePointer(&r.topics[id], unsafe.Pointer(meta))
	return nil
}

func (r *TopicRegistry) snapshotLocked() RegistrySnapshot {
	topics := make(map[string]uint16, len(r.byName))
	for name, id := range r.byName {
		topics[name] = id
	}
	return RegistrySnapshot{
		Version: registryFileVersion,
		Topics:  topics,
		NextID:  atomic.LoadUint32(&r.nextID) + 1,
	}
}

func (r *TopicRegistry) persistLocked() error {
	if r.fileStore == nil {
		return nil
	}
	return r.fileStore.Save(r.snapshotLocked())
}

// Lookup resolves a numeric topic ID without locking so hot handlers stay wait-free.
func (r *TopicRegistry) Lookup(id uint16) (*TopicMetadata, bool) {
	ptr := atomic.LoadPointer(&r.topics[id])
	if ptr == nil {
		return nil, false
	}
	return (*TopicMetadata)(ptr), true
}

// Register assigns or returns a stable ID so batch produce frames avoid repeating topic strings.
func (r *TopicRegistry) Register(name string) (uint16, error) {
	if err := validateTopicName(name); err != nil {
		return 0, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if id, exists := r.byName[name]; exists {
		return id, nil
	}

	var id uint16
	if r.redisStore != nil {
		r.mu.Unlock()
		redisID, err := r.redisStore.Register(context.Background(), name)
		r.mu.Lock()
		if err != nil {
			return 0, err
		}
		id = redisID
		if existing, ok := r.byName[name]; ok {
			if existing != id {
				return 0, fmt.Errorf("topic %q redis/local id conflict: %d vs %d", name, id, existing)
			}
			return existing, nil
		}
	} else {
		next := atomic.AddUint32(&r.nextID, 1)
		if next > 65535 {
			return 0, errors.New("topic registry limit reached (max 65535 topics)")
		}
		id = uint16(next)
	}

	if err := r.installLocked(name, id); err != nil {
		return 0, err
	}
	if err := r.persistLocked(); err != nil {
		return 0, err
	}
	return id, nil
}

// BatchMsgHeader prefixes each message inside a produce-batch payload.
type BatchMsgHeader struct {
	TopicID    uint16
	_          uint16
	PayloadLen uint32
}

// BatchIterator walks batch payloads without per-message heap allocations.
type BatchIterator struct {
	ptr     unsafe.Pointer
	end     unsafe.Pointer
	TopicID uint16
	Payload []byte
}

// NewBatchIterator positions a zero-copy walker over an encoded batch payload.
func NewBatchIterator(payload []byte) BatchIterator {
	if len(payload) == 0 {
		return BatchIterator{}
	}
	start := unsafe.Pointer(&payload[0])
	return BatchIterator{
		ptr: start,
		end: unsafe.Pointer(uintptr(start) + uintptr(len(payload))),
	}
}

// Next advances the iterator and exposes the current topic ID and payload slice.
func (it *BatchIterator) Next() bool {
	if it.ptr == nil || uintptr(it.ptr) >= uintptr(it.end) {
		return false
	}

	remaining := uintptr(it.end) - uintptr(it.ptr)
	if remaining < 8 {
		it.ptr = it.end
		return false
	}

	hdr := (*BatchMsgHeader)(it.ptr)
	totalSize := uintptr(8) + uintptr(hdr.PayloadLen)
	if remaining < totalSize {
		it.ptr = it.end
		return false
	}

	it.TopicID = hdr.TopicID
	if hdr.PayloadLen > 0 {
		it.Payload = unsafe.Slice((*byte)(unsafe.Add(it.ptr, 8)), hdr.PayloadLen)
	} else {
		it.Payload = nil
	}

	it.ptr = unsafe.Pointer(uintptr(it.ptr) + totalSize)
	return true
}

// ReadFrame validates length, CRC, and command before handlers touch payload bytes.
func ReadFrame(r io.Reader, buf []byte, lenBuf []byte) (uint16, uint64, []byte, error) {
	if _, err := io.ReadFull(r, lenBuf[:4]); err != nil {
		return 0, 0, nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:4])

	if len(buf) < int(length) {
		return 0, 0, nil, errors.New("provided buffer too small for frame payload")
	}

	readBuf := buf[:length]
	if _, err := io.ReadFull(r, readBuf); err != nil {
		return 0, 0, nil, err
	}

	return parseFrame(readBuf, length)
}

// ReadFrameTCP reads one frame from a TCP connection without io.Reader boxing.
func ReadFrameTCP(c *net.TCPConn, buf []byte, lenBuf []byte) (uint16, uint64, []byte, error) {
	if err := readFullTCPLoop(c, lenBuf[:4]); err != nil {
		return 0, 0, nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:4])

	if len(buf) < int(length) {
		return 0, 0, nil, errors.New("provided buffer too small for frame payload")
	}

	readBuf := buf[:length]
	if err := readFullTCPLoop(c, readBuf); err != nil {
		return 0, 0, nil, err
	}

	return parseFrame(readBuf, length)
}

func readFullTCPLoop(c *net.TCPConn, buf []byte) error {
	for n := 0; n < len(buf); {
		nn, err := c.Read(buf[n:])
		n += nn
		if err != nil {
			if err == io.EOF && n < len(buf) {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		if nn == 0 {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}

func parseFrame(readBuf []byte, length uint32) (uint16, uint64, []byte, error) {
	if length < 14 {
		return 0, 0, nil, errors.New("frame length too short")
	}

	cmd := binary.BigEndian.Uint16(readBuf[0:2])
	seq := binary.BigEndian.Uint64(readBuf[2:10])
	payload := readBuf[10 : length-4]

	expected := binary.BigEndian.Uint32(readBuf[length-4:])
	calculated := crc32.ChecksumIEEE(payload)
	if calculated != expected {
		return 0, 0, nil, errors.New("checksum verification failed")
	}

	switch cmd {
	case CmdProduce, CmdFetch, CmdProduceBatch, CmdRegisterTopic,
		CmdCommitOffset, CmdCommittedOffset,
		CmdProduceResp, CmdFetchResp, CmdProduceBatchResp, CmdRegisterTopicResp,
		CmdCommitOffsetResp, CmdCommittedOffsetResp:
	default:
		return 0, 0, nil, errors.New("unknown command ID")
	}

	return cmd, seq, payload, nil
}

// readFullBufio fills buf from a pinned bufio.Reader without io.Reader boxing.
func readFullBufio(br *bufio.Reader, buf []byte) error {
	for n := 0; n < len(buf); {
		nn, err := br.Read(buf[n:])
		n += nn
		if err != nil {
			if err == io.EOF && n < len(buf) {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		if nn == 0 {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}

// ReadFrameBufio reads one frame using a reusable bufio.Reader on the TCP hot path.
func ReadFrameBufio(br *bufio.Reader, buf []byte, lenBuf []byte) (uint16, uint64, []byte, error) {
	if err := readFullBufio(br, lenBuf[:4]); err != nil {
		return 0, 0, nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:4])

	if len(buf) < int(length) {
		return 0, 0, nil, errors.New("provided buffer too small for frame payload")
	}

	readBuf := buf[:length]
	if err := readFullBufio(br, readBuf); err != nil {
		return 0, 0, nil, err
	}

	return parseFrame(readBuf, length)
}

// DecodeProduceRequest splits a produce frame into topic, partition, and payload.
func DecodeProduceRequest(payload []byte) (string, uint16, []byte, error) {
	if len(payload) < 4 {
		return "", 0, nil, errors.New("malformed produce request")
	}
	topicLen := binary.BigEndian.Uint16(payload[0:2])
	if len(payload) < 2+int(topicLen)+2 {
		return "", 0, nil, errors.New("malformed produce request: topic length out of bounds")
	}
	topicBytes := payload[2 : 2+topicLen]
	topic := unsafeString(topicBytes)
	partition := binary.BigEndian.Uint16(payload[2+topicLen : 2+topicLen+2])
	msgPayload := payload[2+topicLen+2:]
	return topic, partition, msgPayload, nil
}

// DecodeFetchRequest extracts topic, partition, offset, and byte limit from a fetch frame.
func DecodeFetchRequest(payload []byte) (string, uint16, uint64, uint32, error) {
	if len(payload) < 16 {
		return "", 0, 0, 0, errors.New("malformed fetch request")
	}
	topicLen := binary.BigEndian.Uint16(payload[0:2])
	if len(payload) < 2+int(topicLen)+2+12 {
		return "", 0, 0, 0, errors.New("malformed fetch request: topic length out of bounds")
	}
	topicBytes := payload[2 : 2+topicLen]
	topic := unsafeString(topicBytes)
	pos := 2 + int(topicLen)
	partition := binary.BigEndian.Uint16(payload[pos : pos+2])
	pos += 2
	offset := binary.BigEndian.Uint64(payload[pos : pos+8])
	maxBytes := binary.BigEndian.Uint32(payload[pos+8 : pos+12])
	return topic, partition, offset, maxBytes, nil
}

// EncodeProduceRequest builds a checksummed produce frame into caller-provided buffer space.
func EncodeProduceRequest(buf []byte, seq uint64, topic string, partition uint16, payload []byte) []byte {
	topicBytes := unsafeBytes(topic)
	topicLen := len(topicBytes)
	payloadLen := len(payload)
	framePayloadLen := 2 + topicLen + 2 + payloadLen
	totalLen := uint32(2 + 8 + framePayloadLen + 4)

	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	binary.BigEndian.PutUint16(buf[4:6], CmdProduce)
	binary.BigEndian.PutUint64(buf[6:14], seq)
	binary.BigEndian.PutUint16(buf[14:16], uint16(topicLen))
	copy(buf[16:16+topicLen], topicBytes)
	binary.BigEndian.PutUint16(buf[16+topicLen:16+topicLen+2], partition)
	copy(buf[16+topicLen+2:16+topicLen+2+payloadLen], payload)

	payloadSlice := buf[14 : 14+framePayloadLen]
	checksum := crc32.ChecksumIEEE(payloadSlice)
	binary.BigEndian.PutUint32(buf[14+framePayloadLen:14+framePayloadLen+4], checksum)

	return buf[:4+2+8+framePayloadLen+4]
}

// EncodeFetchRequest builds a checksummed fetch frame into caller-provided buffer space.
func EncodeFetchRequest(buf []byte, seq uint64, topic string, partition uint16, startOffset uint64, maxBytes uint32) []byte {
	topicBytes := unsafeBytes(topic)
	topicLen := len(topicBytes)
	framePayloadLen := 2 + topicLen + 2 + 8 + 4
	totalLen := uint32(2 + 8 + framePayloadLen + 4)

	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	binary.BigEndian.PutUint16(buf[4:6], CmdFetch)
	binary.BigEndian.PutUint64(buf[6:14], seq)
	binary.BigEndian.PutUint16(buf[14:16], uint16(topicLen))
	copy(buf[16:16+topicLen], topicBytes)
	binary.BigEndian.PutUint16(buf[16+topicLen:16+topicLen+2], partition)
	binary.BigEndian.PutUint64(buf[16+topicLen+2:16+topicLen+10], startOffset)
	binary.BigEndian.PutUint32(buf[16+topicLen+10:16+topicLen+14], maxBytes)

	payloadSlice := buf[14 : 14+framePayloadLen]
	checksum := crc32.ChecksumIEEE(payloadSlice)
	binary.BigEndian.PutUint32(buf[14+framePayloadLen:14+framePayloadLen+4], checksum)

	return buf[:4+2+8+framePayloadLen+4]
}

// EncodeProduceResponse returns append status and assigned offset after a produce attempt.
func EncodeProduceResponse(buf []byte, seq uint64, status byte, offset uint64) []byte {
	binary.BigEndian.PutUint32(buf[0:4], 23)
	binary.BigEndian.PutUint16(buf[4:6], CmdProduceResp)
	binary.BigEndian.PutUint64(buf[6:14], seq)
	buf[14] = status
	binary.BigEndian.PutUint64(buf[15:23], offset)

	checksum := crc32.ChecksumIEEE(buf[14:23])
	binary.BigEndian.PutUint32(buf[23:27], checksum)

	return buf[:27]
}

// EncodeFetchResponseHeader prefixes fetch results with status, counts, and log high-water mark.
func EncodeFetchResponseHeader(headerBuf []byte, seq uint64, status byte, msgCount uint32, msgBytes uint32, highWatermark uint64) []byte {
	totalLen := uint32(2 + 8 + FetchRespMetaLen + msgBytes + 4)
	binary.BigEndian.PutUint32(headerBuf[0:4], totalLen)
	binary.BigEndian.PutUint16(headerBuf[4:6], CmdFetchResp)
	binary.BigEndian.PutUint64(headerBuf[6:14], seq)
	headerBuf[14] = status
	binary.BigEndian.PutUint32(headerBuf[15:19], msgCount)
	binary.BigEndian.PutUint64(headerBuf[19:27], highWatermark)
	return headerBuf[:27]
}

// EncodeFetchResponse builds one contiguous fetch frame for a single TCP write.
func EncodeFetchResponse(frameBuf []byte, seq uint64, status byte, msgCount uint32, msgBytes uint32, highWatermark uint64, data []byte) []byte {
	payloadLen := FetchRespMetaLen + len(data)
	totalLen := uint32(2 + 8 + payloadLen + 4)

	binary.BigEndian.PutUint32(frameBuf[0:4], totalLen)
	binary.BigEndian.PutUint16(frameBuf[4:6], CmdFetchResp)
	binary.BigEndian.PutUint64(frameBuf[6:14], seq)
	frameBuf[14] = status
	binary.BigEndian.PutUint32(frameBuf[15:19], msgCount)
	binary.BigEndian.PutUint64(frameBuf[19:27], highWatermark)
	if len(data) > 0 {
		copy(frameBuf[27:27+len(data)], data)
	}

	meta := frameBuf[14:27]
	var checksum uint32
	if len(data) > 0 {
		checksum = crc32.Update(crc32.ChecksumIEEE(meta), crc32.IEEETable, data)
	} else {
		checksum = crc32.ChecksumIEEE(meta)
	}
	binary.BigEndian.PutUint32(frameBuf[14+payloadLen:14+payloadLen+4], checksum)

	return frameBuf[:4+totalLen]
}

// DecodeFetchResponseMeta extracts status, message count, and high watermark from a fetch reply.
func DecodeFetchResponseMeta(payload []byte) (status byte, msgCount uint32, highWatermark uint64, err error) {
	if len(payload) < FetchRespMetaLen {
		return 0, 0, 0, errors.New("malformed fetch response")
	}
	status = payload[0]
	msgCount = binary.BigEndian.Uint32(payload[1:5])
	highWatermark = binary.BigEndian.Uint64(payload[5:13])
	return status, msgCount, highWatermark, nil
}

// EncodeProduceBatchResponse reports batch status, last offset, and how many messages committed.
func EncodeProduceBatchResponse(buf []byte, seq uint64, status byte, offset uint64, committedCount uint32) []byte {
	const payloadLen = ProduceBatchRespMetaLen
	totalLen := uint32(2 + 8 + payloadLen + 4)
	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	binary.BigEndian.PutUint16(buf[4:6], CmdProduceBatchResp)
	binary.BigEndian.PutUint64(buf[6:14], seq)
	buf[14] = status
	binary.BigEndian.PutUint64(buf[15:23], offset)
	binary.BigEndian.PutUint32(buf[23:27], committedCount)

	checksum := crc32.ChecksumIEEE(buf[14 : 14+payloadLen])
	binary.BigEndian.PutUint32(buf[27:31], checksum)

	return buf[:31]
}

// DecodeProduceBatchResponse reads status, offset, and committed count from a batch produce reply.
func DecodeProduceBatchResponse(payload []byte) (status byte, offset uint64, committedCount uint32, err error) {
	if len(payload) < ProduceBatchRespMetaLen {
		return 0, 0, 0, errors.New("malformed produce batch response")
	}
	status = payload[0]
	offset = binary.BigEndian.Uint64(payload[1:9])
	committedCount = binary.BigEndian.Uint32(payload[9:13])
	return status, offset, committedCount, nil
}

// EncodeRegisterTopicRequest registers a topic name and returns its numeric ID on success.
func EncodeRegisterTopicRequest(buf []byte, seq uint64, topic string) []byte {
	topicBytes := unsafeBytes(topic)
	topicLen := len(topicBytes)
	framePayloadLen := 2 + topicLen
	totalLen := uint32(2 + 8 + framePayloadLen + 4)

	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	binary.BigEndian.PutUint16(buf[4:6], CmdRegisterTopic)
	binary.BigEndian.PutUint64(buf[6:14], seq)
	binary.BigEndian.PutUint16(buf[14:16], uint16(topicLen))
	copy(buf[16:16+topicLen], topicBytes)

	payloadSlice := buf[14 : 14+framePayloadLen]
	checksum := crc32.ChecksumIEEE(payloadSlice)
	binary.BigEndian.PutUint32(buf[14+framePayloadLen:14+framePayloadLen+4], checksum)

	return buf[:4+2+8+framePayloadLen+4]
}

// DecodeRegisterTopicRequest extracts the topic name from a registration frame.
func DecodeRegisterTopicRequest(payload []byte) (string, error) {
	if len(payload) < 2 {
		return "", errors.New("malformed register topic request")
	}
	topicLen := binary.BigEndian.Uint16(payload[0:2])
	if len(payload) < 2+int(topicLen) {
		return "", errors.New("malformed register topic request: topic length out of bounds")
	}
	topicBytes := payload[2 : 2+topicLen]
	return string(topicBytes), nil
}

// EncodeRegisterTopicResponse returns registration status and assigned topic ID.
func EncodeRegisterTopicResponse(buf []byte, seq uint64, status byte, topicID uint16) []byte {
	binary.BigEndian.PutUint32(buf[0:4], 17)
	binary.BigEndian.PutUint16(buf[4:6], CmdRegisterTopicResp)
	binary.BigEndian.PutUint64(buf[6:14], seq)
	buf[14] = status
	binary.BigEndian.PutUint16(buf[15:17], topicID)

	checksum := crc32.ChecksumIEEE(buf[14:17])
	binary.BigEndian.PutUint32(buf[17:21], checksum)

	return buf[:21]
}

// DecodeRegisterTopicResponse reads status and topic ID from a registration reply.
func DecodeRegisterTopicResponse(payload []byte) (byte, uint16, error) {
	if len(payload) < 3 {
		return 0, 0, errors.New("malformed register topic response")
	}
	status := payload[0]
	topicID := binary.BigEndian.Uint16(payload[1:3])
	return status, topicID, nil
}

// AppendBatchMessage appends one topic-tagged message into a reusable batch buffer.
func AppendBatchMessage(buf []byte, topicID uint16, payload []byte) []byte {
	start := len(buf)
	newLen := start + 8 + len(payload)
	if cap(buf) < newLen {
		temp := make([]byte, newLen, newLen*2)
		copy(temp, buf)
		buf = temp
	} else {
		buf = buf[:newLen]
	}

	hdr := (*BatchMsgHeader)(unsafe.Pointer(&buf[start]))
	hdr.TopicID = topicID
	hdr.PayloadLen = uint32(len(payload))
	if len(payload) > 0 {
		copy(buf[start+8:newLen], payload)
	}
	return buf
}

// EncodeProduceBatchRequest wraps a prebuilt batch payload in a checksummed frame.
func EncodeProduceBatchRequest(buf []byte, seq uint64, batchPayload []byte) []byte {
	framePayloadLen := len(batchPayload)
	totalLen := uint32(2 + 8 + framePayloadLen + 4)

	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	binary.BigEndian.PutUint16(buf[4:6], CmdProduceBatch)
	binary.BigEndian.PutUint64(buf[6:14], seq)
	copy(buf[14:14+framePayloadLen], batchPayload)

	payloadSlice := buf[14 : 14+framePayloadLen]
	checksum := crc32.ChecksumIEEE(payloadSlice)
	binary.BigEndian.PutUint32(buf[14+framePayloadLen:14+framePayloadLen+4], checksum)

	return buf[:4+2+8+framePayloadLen+4]
}

// unsafeString views topic bytes without copying so decode stays allocation-free on the hot path.
func unsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// unsafeBytes views topic strings without copying so encode stays allocation-free on the hot path.
func unsafeBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
