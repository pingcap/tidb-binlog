package util

import (
	"fmt"
	"net"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/types"
)

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

// DefaultListenAddr returns default listen address with appointed port.
func DefaultListenAddr(port int32) string {
	defaultIP, err := DefaultIP()
	if err != nil {
		log.Infof("get default ip err: %v, use: %s", err, defaultIP)
	}
	return fmt.Sprintf("%s:%d", defaultIP, port)
}

// IsValidateListenHost judge the host is validate listen host or not.
func IsValidateListenHost(host string) bool {
	if host == "127.0.0.1" || host == "localhost" || host == "0.0.0.0" {
		return false
	}
	return true
}

// StdLogger implements samara.StdLogger
type StdLogger struct {
	prefix string
}

// NewStdLogger return an instance of StdLogger
func NewStdLogger(prefix string) *StdLogger {
	return &StdLogger{
		prefix: prefix,
	}
}

// Print implements samara.StdLogger
func (l *StdLogger) Print(v ...interface{}) {
	logger := log.Logger()
	logger.Output(2, l.prefix+fmt.Sprint(v...))
}

// Printf implements samara.StdLogger
func (l *StdLogger) Printf(format string, v ...interface{}) {
	logger := log.Logger()
	logger.Output(2, l.prefix+fmt.Sprintf(format, v...))
}

// Println implements samara.StdLogger
func (l *StdLogger) Println(v ...interface{}) {
	logger := log.Logger()
	logger.Output(2, l.prefix+fmt.Sprintln(v...))
}

// ToColumnTypeMap return a map index by column id
func ToColumnTypeMap(columns []*model.ColumnInfo) map[int64]*types.FieldType {
	colTypeMap := make(map[int64]*types.FieldType)
	for _, col := range columns {
		colTypeMap[col.ID] = &col.FieldType
	}

	return colTypeMap
}
