package compress

import (
	"compress/gzip"
	"io"
	"os"
	"strings"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	pkgfile "github.com/pingcap/tidb-binlog/pkg/file"
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
		return "", errors.NotSupportedf("compression codec %v", codec)
	}
}

func CompressGZIPFile(filename string) (gzipFileName string, err error) {
	var (
		fileLock       *pkgfile.LockedFile
		file, gzipFile *os.File
		gzipWriter     *gzip.Writer
	)

	defer func() {
		if fileLock != nil {
			pkgfile.UnLockFile(fileLock)
		}

		if file != nil {
			file.Close()
		}

		if gzipFile != nil {
			gzipFile.Close()
		}

		if gzipWriter != nil {
			gzipWriter.Close()
		}

		if err != nil && len(gzipFileName) != 0 {
			os.Remove(gzipFileName)
		}
	}()

	fileLock, err = pkgfile.TryLockFile(filename, os.O_WRONLY|os.O_CREATE, pkgfile.PrivateFileMode)
	if err != nil {
		return "", err
	}

	gzipFileName = filename + gzipFileSuffix
	gzipFile, err = os.Create(gzipFileName)
	if err != nil {
		return "", err
	}

	file, err = os.Open(filename)
	if err != nil {
		return "", err
	}

	gzipWriter = gzip.NewWriter(gzipFile)
	if _, err = io.Copy(gzipWriter, file); err != nil {
		return "", err
	}

	if err = os.Remove(filename); err != nil {
		return "", err
	}

	return gzipFileName, err
}

// IsCompressFile returns true if file name end with ".tar.gz"
func IsGzipCompressFile(filename string) bool {
	return strings.HasSuffix(filename, gzipFileSuffix)
}
