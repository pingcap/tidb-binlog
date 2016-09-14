package pump

import (
	"crypto/sha1"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/pkg/file"
)

var (
	errBadBinlogName = errors.New("bad file name")
)

// InitLogger initalizes Pump's logger.
func InitLogger(cfg *Config) {
	if cfg.Debug {
		log.SetLevelByString("debug")
	} else {
		log.SetLevelByString("info")
	}
	log.SetHighlighting(false)
}

func readLocalMachineID(dir string) (string, error) {
	fullPath := filepath.Join(dir, machineDir, machineIDFile)
	if _, err := CheckFileExist(fullPath); err != nil {
		return generateLocalMachineID(dir)
	}

	// read the machine ID from file
	hash, err := ioutil.ReadFile(fullPath)
	if err != nil {
		return "", err
	}
	machID := fmt.Sprintf("%X", hash)
	if len(machID) == 0 {
		return generateLocalMachineID(dir)
	}

	return machID, nil
}

// generate a new machine ID, and save it to file
func generateLocalMachineID(dirpath string) (string, error) {
	rand64 := string(KRand(64, 3))
	log.Debugf("Generated a randomized string with 64 runes, %s", rand64)
	t := sha1.New()
	io.WriteString(t, rand64)
	hash := t.Sum(nil)

	dir := filepath.Join(dirpath, machineDir)
	if _, err := os.Stat(dir); err != nil {
		if !os.IsNotExist(err) {
			return "", err
		}
		// dir not exists, make it
		if err := os.Mkdir(dir, os.ModePerm); err != nil {
			return "", err
		}
	}

	file := filepath.Join(dir, machineIDFile)
	if err := ioutil.WriteFile(file, hash, os.ModePerm); err != nil {
		return "", err
	}
	machID := fmt.Sprintf("%X", hash)
	return machID, nil
}

// KRand is an algorithm that compute rand nums
func KRand(size int, kind int) []byte {
	ikind, kinds, result := kind, [][]int{[]int{10, 48}, []int{26, 97}, []int{26, 65}}, make([]byte, size)
	isAll := kind > 2 || kind < 0
	for i := 0; i < size; i++ {
		if isAll { // random ikind
			ikind = rand.Intn(3)
		}
		scope, base := kinds[ikind][0], kinds[ikind][1]
		result[i] = uint8(base + rand.Intn(scope))
	}
	return result
}

// CheckFileExist chekcs the file exist status and wether it is a file
func CheckFileExist(filepath string) (string, error) {
	fi, err := os.Stat(filepath)
	if err != nil {
		return "", err
	}
	if fi.IsDir() {
		return "", errors.Errorf("filepath: %s, is a directory, not a file", filepath)
	}
	return filepath, nil
}

// Exist checks the dir exist, that it should have some file
func Exist(dirpath string) bool {
	names, err := file.ReadDir(dirpath)
	if err != nil {
		return false
	}

	return len(names) != 0
}

// searchIndex returns the last array index of file
// equal to or smaller than the given index.
func searchIndex(names []string, index uint64) (int, bool) {
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		curIndex, err := parseBinlogName(name)
		if err != nil {
			log.Errorf("parse correct name should never fail: %v", err)
		}

		if index == curIndex {
			return i, true
		}
	}

	return -1, false
}

// isValidBinlog detects the binlog names is valid
func isValidBinlog(names []string) bool {
	var lastSuffix uint64
	for _, name := range names {
		curSuffix, err := parseBinlogName(name)
		if err != nil {
			log.Fatalf("binlogger: parse corrent name should never fail: %v", err)
		}

		if lastSuffix != 0 && lastSuffix != curSuffix-1 {
			return false
		}
		lastSuffix = curSuffix
	}

	return true
}

// readBinlogNames returns sorted filenames in the dirpath
func readBinlogNames(dirpath string) ([]string, error) {
	names, err := file.ReadDir(dirpath)
	if err != nil {
		return nil, err
	}

	fnames := checkBinlogNames(names)
	if len(fnames) == 0 {
		return nil, ErrFileNotFound
	}

	return fnames, nil
}

func checkBinlogNames(names []string) []string {
	var fnames []string
	for _, name := range names {
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
		return 0, errBadBinlogName
	}

	_, err = fmt.Sscanf(str, "binlog-%016d", &index)
	return
}

// the file name format is like binlog-0000000000000001
func fileName(index uint64) string {
	return fmt.Sprintf("binlog-%016d", index)
}
