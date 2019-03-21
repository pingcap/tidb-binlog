package util

import (
	"fmt"
	"net"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/security"
	"github.com/pingcap/tidb/kv"
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

// RetryOnError defines a action with retry when fn returns error
func RetryOnError(retryCount int, sleepTime time.Duration, errStr string, fn func() error) error {
	var err error
	for i := 0; i < retryCount; i++ {
		err = fn()
		if err == nil {
			break
		}

		log.Errorf("%s: %v", errStr, err)
		time.Sleep(sleepTime)
	}

	return errors.Trace(err)
}

// QueryLatestTsFromPD returns the latest ts
func QueryLatestTsFromPD(tiStore kv.Storage) (int64, error) {
	version, err := tiStore.CurrentVersion()
	if err != nil {
		log.Errorf("get current version error: %v", err)
		return 0, errors.Trace(err)
	}

	return int64(version.Ver), nil
}

func GetPdClient(etcdURLs string, securityConfig security.Config) (pd.Client, error) {
	urlv, err := flags.NewURLsValue(etcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pdReconnTimes := 30

	var pdCli pd.Client
	for i := 1; i < pdReconnTimes; i++ {
		pdCli, err = pd.NewClient(urlv.StringSlice(), pd.SecurityOption{
			CAPath:   securityConfig.SSLCA,
			CertPath: securityConfig.SSLCert,
			KeyPath:  securityConfig.SSLKey,
		})
		if err != nil {
			time.Sleep(time.Duration(pdReconnTimes*i) * time.Millisecond)
		} else {
			break
		}
	}

	return pdCli, errors.Trace(err)
}
