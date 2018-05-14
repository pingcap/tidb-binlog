package util

import (
	"errors"
	"fmt"
	"net"

	"github.com/ngaut/log"
)

// TsToTimestamp translate ts to timestamp
func TsToTimestamp(ts int64) int64 {
	return ts >> 18 / 1000
}

// DefaultIP get a default non local ip, err is not nil, ip return 127.0.0.1
func DefaultIP() (ip string, err error) {
	ip = "127.0.0.1"

	ifaces, err := net.Interfaces()
	if err != nil {
		return
	}

	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip.IsUnspecified() || ip.IsLoopback() {
				continue
			}

			ip = ip.To4()
			if ip == nil {
				continue
			}

			return ip.String(), nil
		}
	}

	err = errors.New("no ip found")
	return
}

type StdLogger struct {
	prefix string
}

func NewStdLogger(prefix string) *StdLogger {
	return &StdLogger{
		prefix: prefix,
	}
}

func (l *StdLogger) Print(v ...interface{}) {
	logger := log.Logger()
	logger.Output(2, l.prefix+fmt.Sprint(v...))
}

func (l *StdLogger) Printf(format string, v ...interface{}) {
	logger := log.Logger()
	logger.Output(2, l.prefix+fmt.Sprintf(format, v...))
}

func (l *StdLogger) Println(v ...interface{}) {
	logger := log.Logger()
	logger.Output(2, l.prefix+fmt.Sprintln(v...))
}
