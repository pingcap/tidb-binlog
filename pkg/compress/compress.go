package compress

import (
	"bytes"
	"compress/gzip"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

type CompressionCodec int8

const (
	CompressionNone CompressionCodec = iota
	CompressionGZIP
)

// ToCompressionCodec converts v to CompressionCodec.
func ToCompressionCodec(v string) CompressionCodec {
	v = strings.ToLower(v)
	switch v {
	case "":
		return CompressionNone
	case "gzip":
		return CompressionGZIP
	default:
		log.Warnf("unknown codec %v, no compression.", v)
		return CompressionNone
	}
}

// Compress compresses payload based on the codec.
func Compress(data []byte, codec CompressionCodec) (payload []byte, err error) {
	switch codec {
	case CompressionNone:
		payload = data
	case CompressionGZIP:
		var buf bytes.Buffer
		writer := gzip.NewWriter(&buf)
		if _, err := writer.Write(data); err != nil {
			return nil, errors.Trace(err)
		}
		if err := writer.Close(); err != nil {
			return nil, errors.Trace(err)
		}
		payload = buf.Bytes()
	}

	return payload, nil
}
