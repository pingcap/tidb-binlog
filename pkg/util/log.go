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
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Log prints log only after a certain amount of time
type Log struct {
	sync.RWMutex
	lastTime map[string]time.Time
	interval map[string]time.Duration
}

// NewLog returns a new Log
func NewLog() *Log {
	return &Log{
		lastTime: make(map[string]time.Time),
		interval: make(map[string]time.Duration),
	}
}

// Add adds new label
func (l *Log) Add(label string, interval time.Duration) {
	l.Lock()
	l.interval[label] = interval
	l.Unlock()
}

// Print executes the fn to print log
func (l *Log) Print(label string, fn func()) {
	l.Lock()
	defer l.Unlock()

	_, ok := l.lastTime[label]
	if !ok || time.Since(l.lastTime[label]) > l.interval[label] {
		fn()
		l.lastTime[label] = time.Now()
	}
}

// _globalP is the global ZapProperties in log
var _globalP *log.ZapProperties

// InitLogger initializes logger
func InitLogger(level string, file string) error {
	cfg := &log.Config{
		Level: level,
		File: log.FileLogConfig{
			Filename:  file,
			LogRotate: true,
			// default rotate by size 300M in pingcap/log never delete old files
			// MaxSize:
			// MaxDays:
			// MaxBackups:
		},
	}

	var lg *zap.Logger
	var err error
	lg, _globalP, err = log.InitLogger(cfg)
	if err != nil {
		return err
	}

	// Do not log stack traces at all, as we'll get the stack trace from the
	// error itself.
	lg = lg.WithOptions(zap.AddStacktrace(zap.DPanicLevel))

	log.ReplaceGlobals(lg, _globalP)

	sarama.Logger = NewStdLogger("[sarama] ")
	return nil
}

// LogHook to get the save entrys for test
type LogHook struct {
	// save the log entrys
	Entrys       []zapcore.Entry
	originLogger *zap.Logger
}

// SetUp LogHook
func (h *LogHook) SetUp() {
	h.originLogger = log.L()
	lg := log.L().WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		h.Entrys = append(h.Entrys, entry)
		return nil
	}))

	log.ReplaceGlobals(lg, _globalP)
}

// TearDown set back the origin logger
func (h *LogHook) TearDown() {
	log.ReplaceGlobals(h.originLogger, _globalP)
}
