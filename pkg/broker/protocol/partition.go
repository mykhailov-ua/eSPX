package protocol

import (
	"strconv"
	"strings"
)

// TopicPartitionID is the stable storage and coordination key for one topic partition.
func TopicPartitionID(topic string, partition uint16) string {
	var b strings.Builder
	b.Grow(len(topic) + 8)
	b.WriteString(topic)
	b.WriteByte('/')
	b.WriteString(strconv.FormatUint(uint64(partition), 10))
	return b.String()
}

// ParseTopicPartitionID splits a storage/coordination key into topic name and partition index.
func ParseTopicPartitionID(tpKey string) (topic string, partition uint16) {
	i := strings.LastIndex(tpKey, "/")
	if i < 0 {
		return tpKey, 0
	}
	p, err := strconv.ParseUint(tpKey[i+1:], 10, 16)
	if err != nil {
		return tpKey, 0
	}
	return tpKey[:i], uint16(p)
}
