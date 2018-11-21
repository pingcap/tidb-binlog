package compress

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

// CompressionCodec defines type of compression.
type CompressionCodec int8

const (
	// CompressionNone means no compression.
	CompressionNone CompressionCodec = iota
	// CompressionGZIP means using GZIP compression.
	CompressionGZIP
	// CompressionFlate means using FLATE compression.
	CompressionFlate

	// DefaultCompressLevel means the default compress level of flate, levels range from 1 (BestSpeed) to 9 (BestCompression);
	// higher levels typically run slower but compress more.
	DefaultCompressLevel = 1
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
	var buf bytes.Buffer
	switch codec {
	case CompressionNone:
		payload = data
	case CompressionGZIP:
		writer := gzip.NewWriter(&buf)
		if _, err := writer.Write(data); err != nil {
			return nil, errors.Trace(err)
		}
		if err := writer.Close(); err != nil {
			return nil, errors.Trace(err)
		}
		payload = buf.Bytes()
	case CompressionFlate:
		writer, err := flate.NewWriter(&buf, DefaultCompressLevel)
		if err != nil {
			return nil, errors.Trace(err)
		}
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
