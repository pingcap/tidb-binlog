package slicer

import (
	"github.com/Shopify/sarama"
)

var (
	// MessageID is ID to indicate which binlog it belongs
	MessageID = []byte("messageID")
	// No is index of slices of binlog
	No = []byte("No")
	// Total is total number of binlog slices
	Total = []byte("total")
	// Checksum is checksum code of binlog payload
	// to save space, it's only in last binlog slice
	Checksum = []byte("checksum")
)

// GetValueFromComsumerMessageHeader gets value from message header
func GetValueFromComsumerMessageHeader(key []byte, message *sarama.ConsumerMessage) []byte {
	for _, record := range message.Headers {
		if string(record.Key) == string(key) {
			return record.Value
		}
	}
	return nil
}
