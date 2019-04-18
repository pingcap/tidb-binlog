package compress

import (
	"compress/gzip"
	"fmt"
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

// CompressFile compresses a file, and return the compressed file name
func CompressFileWithTS(filename string, codec CompressionCodec, ts int64) error {
	switch codec {
	case CompressionNone:
		return nil
	case CompressionGZIP:
		compressFilename := GetCompressFileNameWithTS(filename, gzipFileSuffix, ts)
		return CompressGZIPFile(filename, compressFilename)
	default:
		return errors.NotSupportedf("compression codec %v", codec)
	}
}

// CompressGZIPFile compresses file by gzip
func CompressGZIPFile(filename, compressFilename string) (err error) {
	var (
		fileLock       *pkgfile.LockedFile
		file, gzipFile *os.File
		gzipWriter     *gzip.Writer
	)

	defer func() {
		if fileLock != nil {
			if err1 := pkgfile.UnLockFile(fileLock); err1 != nil {
				log.Warnf("unlock file %s failed %v", fileLock.Name(), err1)
			}
		}

		if file != nil {
			if err1 := file.Close(); err1 != nil {
				log.Warnf("close file %s failed %v", file.Name(), err1)
			}
		}

		if gzipWriter != nil {
			if err1 := gzipWriter.Close(); err1 != nil {
				log.Warnf("close gzip writer %s failed %v", compressFilename, err1)
			}
		}

		if gzipFile != nil {
			if err1 := gzipFile.Close(); err1 != nil {
				log.Warnf("close file %s failed %v", compressFilename, err1)
			}
		}

		if err != nil {
			if err1 := os.Remove(compressFilename); err1 != nil {
				log.Warnf("remove file %s failed %v", compressFilename, err1)
			}
		}
	}()

	if err = os.Remove(compressFilename); err != nil {
		if _, ok := err.(*os.PathError); !ok {
			return err
		}
	}

	fileLock, err = pkgfile.TryLockFile(filename, os.O_WRONLY|os.O_CREATE, pkgfile.PrivateFileMode)
	if err != nil {
		return err
	}

	gzipFile, err = os.Create(compressFilename)
	if err != nil {
		return err
	}

	file, err = os.Open(filename)
	if err != nil {
		return err
	}

	gzipWriter = gzip.NewWriter(gzipFile)
	if _, err = io.Copy(gzipWriter, file); err != nil {
		return err
	}

	if err = os.Remove(filename); err != nil {
		return err
	}

	return err
}

// IsCompressFile returns true if file is compressed.
func IsCompressFile(filename string) bool {
	// now only support gzip
	return IsGzipCompressFile(filename)
}

// IsCompressFile returns true if file name ends with ".tar.gz"
func IsGzipCompressFile(filename string) bool {
	return strings.HasSuffix(filename, gzipFileSuffix)
}

// GetCompressFileNameWithTS returns a new filename with ts and suffix
func GetCompressFileNameWithTS(filename, suffix string, ts int64) string {
	return fmt.Sprintf("%s-%d%s", filename, ts, suffix)
}
