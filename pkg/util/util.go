// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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
			if ipStr := getAddrDefaultIP(addr); len(ipStr) > 0 {
				return ipStr, nil
			}
		}
	}

	err = errors.New("no ip found")
	return
}

func getAddrDefaultIP(addr net.Addr) string {
	var ip net.IP
	switch v := addr.(type) {
	case *net.IPNet:
		ip = v.IP
	case *net.IPAddr:
		ip = v.IP
	}
	if ip.IsUnspecified() || ip.IsLoopback() {
		return ""
	}

	ip = ip.To4()
	if ip == nil {
		return ""
	}

	return ip.String()
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
	if ip := net.ParseIP(host); ip != nil {
		return !ip.IsLoopback()
	}
	return host != "localhost"
}

// StdLogger implements samara.StdLogger
type StdLogger struct {
	prefix string
}

// NewStdLogger return an instance of StdLogger
func NewStdLogger(prefix string) StdLogger {
	return StdLogger{
		prefix: prefix,
	}
}

func (l StdLogger) emit(msg string) {
	logger := log.Logger()
	logger.Output(2, l.prefix+msg)
}

// Print implements samara.StdLogger
func (l StdLogger) Print(v ...interface{}) {
	l.emit(fmt.Sprint(v...))
}

// Printf implements samara.StdLogger
func (l StdLogger) Printf(format string, v ...interface{}) {
	l.emit(fmt.Sprintf(format, v...))
}

// Println implements samara.StdLogger
func (l StdLogger) Println(v ...interface{}) {
	l.emit(fmt.Sprintln(v...))
}

// ToColumnTypeMap return a map index by column id
func ToColumnTypeMap(columns []*model.ColumnInfo) map[int64]*types.FieldType {
	colTypeMap := make(map[int64]*types.FieldType, len(columns))
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

// Store the function in a variable so that we can mock it when testing
var newPdCli = pd.NewClient

// GetPdClient create a PD client
func GetPdClient(etcdURLs string, securityConfig security.Config) (pd.Client, error) {
	urlv, err := flags.NewURLsValue(etcdURLs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	pdReconnTimes := 30

	var pdCli pd.Client
	for i := 1; i < pdReconnTimes; i++ {
		pdCli, err = newPdCli(urlv.StringSlice(), pd.SecurityOption{
			CAPath:   securityConfig.SSLCA,
			CertPath: securityConfig.SSLCert,
			KeyPath:  securityConfig.SSLKey,
		})
		if err == nil {
			break
		}
		time.Sleep(time.Duration(pdReconnTimes*i) * time.Millisecond)
	}

	return pdCli, errors.Trace(err)
}
