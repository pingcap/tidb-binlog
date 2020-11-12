// Copyright 2020 PingCAP, Inc.
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

package relayprinter

import (
	"context"
	"fmt"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"github.com/pingcap/tipb/go-binlog"
)

// Printer is used to print the content of relay log files.
type Printer struct {
	cfg    *Config
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// NewPrinter creates a new Printer instance.
func NewPrinter(cfg *Config) *Printer {
	return &Printer{
		cfg: cfg,
	}
}

// Process prints contents of relay log files.
func (p *Printer) Process() error {
	var suffix uint64
	if p.cfg.File != "" {
		var err error
		suffix, _, err = binlogfile.ParseBinlogName(p.cfg.File)
		if err != nil {
			return errors.Trace(err)
		}
	}

	binlogger, err := binlogfile.OpenBinlogger(p.cfg.Dir, binlogfile.SegmentSizeBytes)
	if err != nil {
		return errors.Trace(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	bCh, eCh := binlogger.ReadAll(ctx)

	p.wg.Add(1)
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case blg := <-bCh:
			if suffix != 0 {
				if blg.Pos.Suffix < suffix {
					// NOTE: `binlogger.ReadFrom` is not suitable to read all binlog items from the specified file,
					// so we use `binlogger.ReadAll` to read all of them but do not process them.
					continue
				} else if blg.Pos.Suffix > suffix {
					return nil // all contents in the specified file have been processed.
				}
			}
			p.print(blg)
		case err2 := <-eCh:
			// abort process for any error.
			return err2
		}
	}

	return nil
}

// Close closes the Printer instance.
func (p *Printer) Close() {
	if p.cancel != nil {
		p.cancel()
		p.cancel = nil
	}
	p.wg.Wait()
}

func (p *Printer) print(blg *binlog.Entity) {
	fmt.Println(blg)
}
