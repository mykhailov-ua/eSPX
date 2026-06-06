package protocol

import (
	"encoding/binary"
	"errors"
	"io"
	"unsafe"
)

const (
	CmdProduce     uint16 = 1
	CmdFetch       uint16 = 2
	CmdProduceResp uint16 = 101
	CmdFetchResp   uint16 = 102
)

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

	cmd := binary.BigEndian.Uint16(readBuf[0:2])
	seq := binary.BigEndian.Uint64(readBuf[2:10])
	payload := readBuf[10:]

	return cmd, seq, payload, nil
}

func DecodeProduceRequest(payload []byte) (string, []byte, error) {
	if len(payload) < 2 {
		return "", nil, errors.New("malformed produce request")
	}
	topicLen := binary.BigEndian.Uint16(payload[0:2])
	if len(payload) < 2+int(topicLen) {
		return "", nil, errors.New("malformed produce request: topic length out of bounds")
	}
	topicBytes := payload[2 : 2+topicLen]
	topic := unsafeString(topicBytes)
	msgPayload := payload[2+topicLen:]
	return topic, msgPayload, nil
}

func DecodeFetchRequest(payload []byte) (string, uint64, uint32, error) {
	if len(payload) < 14 {
		return "", 0, 0, errors.New("malformed fetch request")
	}
	topicLen := binary.BigEndian.Uint16(payload[0:2])
	if len(payload) < 2+int(topicLen)+12 {
		return "", 0, 0, errors.New("malformed fetch request: topic length out of bounds")
	}
	topicBytes := payload[2 : 2+topicLen]
	topic := unsafeString(topicBytes)
	offset := binary.BigEndian.Uint64(payload[2+topicLen : 2+topicLen+8])
	maxBytes := binary.BigEndian.Uint32(payload[2+topicLen+8 : 2+topicLen+12])
	return topic, offset, maxBytes, nil
}

func EncodeProduceRequest(buf []byte, seq uint64, topic string, payload []byte) []byte {
	topicBytes := unsafeBytes(topic)
	topicLen := len(topicBytes)
	payloadLen := len(payload)
	framePayloadLen := 2 + topicLen + payloadLen
	totalLen := uint32(2 + 8 + framePayloadLen)

	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	binary.BigEndian.PutUint16(buf[4:6], CmdProduce)
	binary.BigEndian.PutUint64(buf[6:14], seq)
	binary.BigEndian.PutUint16(buf[14:16], uint16(topicLen))
	copy(buf[16:16+topicLen], topicBytes)
	copy(buf[16+topicLen:16+topicLen+payloadLen], payload)

	return buf[:4+2+8+framePayloadLen]
}

func EncodeFetchRequest(buf []byte, seq uint64, topic string, startOffset uint64, maxBytes uint32) []byte {
	topicBytes := unsafeBytes(topic)
	topicLen := len(topicBytes)
	framePayloadLen := 2 + topicLen + 8 + 4
	totalLen := uint32(2 + 8 + framePayloadLen)

	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	binary.BigEndian.PutUint16(buf[4:6], CmdFetch)
	binary.BigEndian.PutUint64(buf[6:14], seq)
	binary.BigEndian.PutUint16(buf[14:16], uint16(topicLen))
	copy(buf[16:16+topicLen], topicBytes)
	binary.BigEndian.PutUint64(buf[16+topicLen:16+topicLen+8], startOffset)
	binary.BigEndian.PutUint32(buf[16+topicLen+8:16+topicLen+12], maxBytes)

	return buf[:4+2+8+framePayloadLen]
}

func EncodeProduceResponse(buf []byte, seq uint64, status byte, offset uint64) []byte {
	binary.BigEndian.PutUint32(buf[0:4], 19)
	binary.BigEndian.PutUint16(buf[4:6], CmdProduceResp)
	binary.BigEndian.PutUint64(buf[6:14], seq)
	buf[14] = status
	binary.BigEndian.PutUint64(buf[15:23], offset)
	return buf[:23]
}

func EncodeFetchResponseHeader(headerBuf []byte, seq uint64, status byte, msgCount uint32, msgBytes uint32) []byte {
	totalLen := uint32(2 + 8 + 1 + 4 + msgBytes)
	binary.BigEndian.PutUint32(headerBuf[0:4], totalLen)
	binary.BigEndian.PutUint16(headerBuf[4:6], CmdFetchResp)
	binary.BigEndian.PutUint64(headerBuf[6:14], seq)
	headerBuf[14] = status
	binary.BigEndian.PutUint32(headerBuf[15:19], msgCount)
	return headerBuf[:19]
}

func unsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func unsafeBytes(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
