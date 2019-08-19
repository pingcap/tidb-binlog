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
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-binlog/pkg/flags"
	"github.com/pingcap/tidb-binlog/pkg/security"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
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
		log.Info("get default ip failed", zap.Error(err), zap.String("use", defaultIP))
	}
	return fmt.Sprintf("%s:%d", defaultIP, port)
}

// IsValidateListenHost judge the host is validate listen host or not.
func IsValidateListenHost(host string) bool {
	if len(host) == 0 {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsLoopback() {
			return false
		}
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
	logger := log.L().WithOptions(zap.AddCallerSkip(2))
	// log as info level
	logger.Sugar().Info(l.prefix + msg)
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
	l.emit(fmt.Sprint(v...))
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

		log.Error(errStr, zap.Error(err))
		time.Sleep(sleepTime)
	}

	return errors.Trace(err)
}

// RetryContext retries the specified `fn` until it returns no error or the context is canceled,
// for at most `retryCount` times.
// The wait time before the `i`th retry is calculated with `sleepTime` * (`backoffFactor` ** i).
func RetryContext(ctx context.Context, retryCount int, sleepTime time.Duration, backoffFactor int, fn func(context.Context) error) error {
	var err error
	for i := 0; i < retryCount; i++ {
		err = fn(ctx)
		if err == nil {
			break
		}

		select {
		case <-time.After(sleepTime):
		case <-ctx.Done():
			return err
		}
		sleepTime = sleepTime * time.Duration(backoffFactor)
	}
	return err
}

// StrictDecodeFile decodes the toml file strictly. If any item in confFile file is not mapped
// into the Config struct, issue an error and stop the server from starting.
func StrictDecodeFile(path, component string, cfg interface{}) error {
	metaData, err := toml.DecodeFile(path, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	if undecoded := metaData.Undecoded(); len(undecoded) > 0 {
		var undecodedItems []string
		for _, item := range undecoded {
			undecodedItems = append(undecodedItems, item.String())
		}
		err = errors.Errorf("component %s's config file %s contained unknown configuration options: %s",
			component, path, strings.Join(undecodedItems, ", "))
	}

	return errors.Trace(err)
}

// TryUntilSuccess retries the given function until error is nil or the context is done,
// waiting for `waitInterval` time between retries.
func TryUntilSuccess(ctx context.Context, waitInterval time.Duration, errMsg string, fn func() error) error {
	for {
		if err := fn(); err != nil {
			if errMsg != "" {
				log.Error(errMsg, zap.Error(err))
			}

			select {
			case <-time.After(waitInterval):
				continue
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			}
		}

		return nil
	}
}

// QueryLatestTsFromPD returns the latest ts
func QueryLatestTsFromPD(tiStore kv.Storage) (int64, error) {
	version, err := tiStore.CurrentVersion()
	if err != nil {
		log.Error("get current version failed", zap.Error(err))
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

// AdjustString adjusts v to default value if v is nil
func AdjustString(v *string, defValue string) {
	if len(*v) == 0 {
		*v = defValue
	}
}

// AdjustInt adjusts v to default value if v is nil
func AdjustInt(v *int, defValue int) {
	if *v == 0 {
		*v = defValue
	}
}

// AdjustDuration adjusts v to default value if v is nil
func AdjustDuration(v *time.Duration, defValue time.Duration) {
	if *v == 0 {
		*v = defValue
	}
}
