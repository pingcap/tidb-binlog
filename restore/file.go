package restore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/file"
)

var magic uint32 = 471532804
var crcTable = crc32.MakeTable(crc32.Castagnoli)

//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
// | magic word (4 byte)| Size (8 byte, len(payload)) |    payload    |  crc  |
//  - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
func readBinlog(br *bufio.Reader) ([]byte, error) {
	// read and chekc magic number
	magicNum, err := readInt32(br)
	if err == io.EOF {
		return nil, io.EOF
	}
	checkMagic(magicNum)
	// read payload length
	size, err := readInt64(br)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	// size+4 = len(payload)+len(crc)
	data := make([]byte, size+4)
	// read payload+crc
	if _, err = io.ReadFull(br, data); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	payload := data[:size]
	// crc32 check
	entryCrc := binary.LittleEndian.Uint32(data[size:])
	crc := crc32.Checksum(payload, crcTable)
	if crc != entryCrc {
		log.Fatalf("crc32 %v doesn't match %v", crc, entryCrc)
	}
	return payload, nil
}

func checkMagic(magicNum uint32) {
	if magicNum != magic {
		log.Fatalf("crc32 %d don't match %d", magicNum, magic)
	}
}

func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}

func readInt32(r io.Reader) (uint32, error) {
	var n uint32
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}

func searchFileIndex(names []string, name string) int {
	for i := len(names) - 1; i >= 0; i-- {
		if name == names[i] {
			return i
		}
	}

	log.Fatalf("file %s not found", name)
	return 0
}

// readBinlogNames returns sorted filenames in the dirpath
func readBinlogNames(dirpath string) ([]string, error) {
	names, err := file.ReadDir(dirpath)
	if err != nil {
		return nil, err
	}

	fnames := checkBinlogNames(names)
	if len(fnames) == 0 {
		return nil, errors.NotFoundf("dir %s", dirpath)
	}

	return fnames, nil
}

func checkBinlogNames(names []string) []string {
	var fnames []string
	for _, name := range names {
		if strings.HasSuffix(name, "savepoint") {
			continue
		}
		if _, err := parseBinlogName(name); err != nil {
			if !strings.HasSuffix(name, ".tmp") {
				log.Warningf("ignored file %v in wal", name)
			}
			continue
		}
		fnames = append(fnames, name)
	}

	return fnames
}

func parseBinlogName(str string) (index uint64, err error) {
	if !strings.HasPrefix(str, "binlog-") {
		log.Errorf("bad file name %s", str)
		return 0, errors.Errorf("bad file name %s", str)
	}

	_, err = fmt.Sscanf(str, "binlog-%016d", &index)
	return
}

// the file name format is like binlog-0000000000000001
func fileName(index uint64) string {
	return fmt.Sprintf("binlog-%016d", index)
}
