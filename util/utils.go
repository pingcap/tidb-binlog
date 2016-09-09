package util

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"strconv"
)

const (
	KC_RAND_KIND_NUM   = 0
	KC_RAND_KIND_LOWER = 1
	KC_RAND_KIND_UPPER = 2
	KC_RAND_KIND_ALL   = 3
)

var (
	cmddir  string
	rootdir string
	datadir string
)

func init() {
	SetCmdDir()
	SetRootDir()
}

func SetCmdDir() {
	file, _ := exec.LookPath(os.Args[0])
	path, _ := filepath.Abs(file)
	cmddir = filepath.Dir(path)

}

func SetRootDir() {
	path, _ := os.Getwd()
	if filepath.Base(path) == "bin" {
		rootdir = filepath.Dir(path)
	} else {
		rootdir = path
	}
}

func SetDataDir(d string) {
	if d == "" {
		datadir = rootdir
		return
	}
	datadir = d
}

func GetCmdDir() string {
	return cmddir
}

func GetRootDir() string {
	return rootdir
}

func GetDataDir() string {
	return datadir
}

func CheckFileExist(filepath string) (string, error) {
	fi, err := os.Stat(filepath)
	if err != nil {
		return "", err
	}
	if fi.IsDir() {
		return "", errors.New(fmt.Sprintf("filepath: %s, is a directory, not a file", filepath))
	}
	return filepath, nil
}

func KRand(size int, kind int) []byte {
	ikind, kinds, result := kind, [][]int{[]int{10, 48}, []int{26, 97}, []int{26, 65}}, make([]byte, size)
	is_all := kind > 2 || kind < 0
	for i := 0; i < size; i++ {
		if is_all { // random ikind
			ikind = rand.Intn(3)
		}
		scope, base := kinds[ikind][0], kinds[ikind][1]
		result[i] = uint8(base + rand.Intn(scope))
	}
	return result
}

func IntranetIP() (ips []string, err error) {
       	ips = make([]string, 0)
       	ifaces, e := net.Interfaces()
       	if e != nil {
       		return ips, e
       	}
       	for _, iface := range ifaces {
       		if iface.Flags&net.FlagUp == 0 {
       			continue // interface down
       		}
       		if iface.Flags&net.FlagLoopback != 0 {
       			continue // loopback interface
       		}
       		// ignore docker and warden bridge
       		if strings.HasPrefix(iface.Name, "docker") || strings.HasPrefix(iface.Name, "w-") {
       			continue
       		}
       		addrs, e := iface.Addrs()
       		if e != nil {
       			return ips, e
       		}
       		for _, addr := range addrs {
       			var ip net.IP
       			switch v := addr.(type) {
       			case *net.IPNet:
       				ip = v.IP
       			case *net.IPAddr:
       				ip = v.IP
       			}
       			if ip == nil || ip.IsLoopback() {
       				continue
       			}
       			ip = ip.To4()
       			if ip == nil {
       				continue // not an ipv4 address
       			}
       			ipStr := ip.String()
       			if IsIntranet(ipStr) {
       				ips = append(ips, ipStr)
       			}
       		}
       	}
       	return ips, nil
}

func IsIntranet(ipStr string) bool {
       	if strings.HasPrefix(ipStr, "10.") || strings.HasPrefix(ipStr, "192.168.") {
       		return true
       	}
       	if strings.HasPrefix(ipStr, "172.") {
       		// 172.16.0.0-172.31.255.255
       		arr := strings.Split(ipStr, ".")
       		if len(arr) != 4 {
       			return false
       		}
       		second, err := strconv.ParseInt(arr[1], 10, 64)
       		if err != nil {
       			return false
       		}
       		if second >= 16 && second <= 31 {
       			return true
       		}
       	}
       	return false
}
