package compress

import (
	"os"
	"io"
	"strings"
	"compress/gzip"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/file"
	"github.com/pingcap/errors"
)

// CompressionCodec defines type of compression.
type CompressionCodec int8

const (
	// CompressionNone means no compression.
	CompressionNone CompressionCodec = iota
	// CompressionGZIP means using GZIP compression.
	CompressionGZIP

	gzipFileSuffix = ".tar.gz"
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

func CompressFile(filename string, codec CompressionCodec) (string, error) {
	switch codec {
	case CompressionNone:
		return filename, nil
	case CompressionGZIP:
		return CompressGZIPFile(filename)
	default:
		return "", errors.NotSupported("compression codec %v", codec)
	}
}

func CompressGZIPFile(filename string) (string, error) {
	fileLock, err := file.TryLockFile(filename, os.O_WRONLY|os.O_CREATE, file.PrivateFileMode)
	if err != nil {
		return "", err
	}
	defer file.UnLockFile(fileLock)

	newGzipFileName := filename + gzipFileSuffix
	newGzipFile, err := os.Create(newGzipFileName)
	if err != nil {
		return "", err
	}
	defer newGzipFile.Close()
	
	gzipWriter := gzip.NewWriter(newGzipFile)
	defer gzipWriter.Close()
	
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()
		
	if _, err = io.Copy(gzipWriter, file); err != nil {
		return "", err
	}

	if err = os.Remove(filename); err != nil {
		return "", err
	}

	return newGzipFileName, err
} 

// IsCompressFile returns true if file name end with ".tar.gz"
func IsGzipCompressFile(filename string) bool {
	return strings.HasSuffix(filename, gzipFileSuffix)
}