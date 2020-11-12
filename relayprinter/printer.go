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
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-binlog/pkg/binlogfile"
	"github.com/pingcap/tidb-binlog/pkg/loader"
	obinlog "github.com/pingcap/tidb-tools/tidb-binlog/slave_binlog_proto/go-binlog"
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
		case blg, ok := <-bCh:
			if !ok {
				return nil
			}
			if suffix != 0 {
				if blg.Pos.Suffix < suffix {
					// NOTE: `binlogger.ReadFrom` is not suitable to read all binlog items from the specified file,
					// so we use `binlogger.ReadAll` to read all of them but do not process them.
					continue
				} else if blg.Pos.Suffix > suffix {
					return nil // all contents in the specified file have been processed.
				}
			}
			if err = p.print(blg); err != nil {
				return err
			}
		case err = <-eCh:
			// abort process for any error.
			return err
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

// print prints the contents of binlog entity.
func (p *Printer) print(blg *binlog.Entity) error {
	oblg := new(obinlog.Binlog)
	if err := oblg.Unmarshal(blg.Payload); err != nil {
		return err
	}

	txn, err := loader.SecondaryBinlogToTxn(oblg)
	if err != nil {
		return err
	}
	txn.Metadata = oblg.CommitTs

	// TODO: filter

	fmt.Printf("# at %s\n", blg.Pos.String())
	fmt.Printf("# commitTS:%d\n", oblg.CommitTs)

	switch oblg.Type {
	case obinlog.BinlogType_DDL:
		fmt.Printf("# schema:%s table:%s\n", *oblg.DdlData.SchemaName, *oblg.DdlData.TableName)
		fmt.Printf("%s\n\n", oblg.DdlData.DdlQuery)
	case obinlog.BinlogType_DML:
		for _, dml := range txn.DMLs {
			fmt.Printf("# schema: %s table:%s\n", dml.Database, dml.Table)

			cols := dml.ColumnNames()
			values := make([]interface{}, 0, len(cols))
			valuesStr := make([]string, 0, len(cols))
			for _, col := range cols {
				values = append(values, dml.Values[col])
				valuesStr = append(valuesStr, fmt.Sprintf("%v", dml.Values[col]))
			}

			switch dml.Tp {
			case loader.InsertDMLType:
				fmt.Println("# INSERET")
				fmt.Printf("# values: %v\n", dml.Values)
				fmt.Printf("INSERT INTO %s\n  (%s)\nVALUES\n  (%v)\n", dml.TableName(), loader.BuildColumnList(cols), strings.Join(valuesStr, ","))
			case loader.UpdateDMLType:
				oldValues := make([]interface{}, 0, len(cols))
				for _, col := range cols {
					oldValues = append(oldValues, dml.OldValues[col])
				}

				fmt.Println("# UPDATE")
				fmt.Printf("# old alues: %v\n", dml.OldValues)
				fmt.Printf("# new values: %v\n", dml.Values)
				fmt.Printf("UPDATE %s SET\n", dml.TableName())
				for i, col := range cols {
					fmt.Printf("  %s = %s", loader.QuoteName(col), valuesStr[i])
					if i < len(cols)-1 {
						fmt.Printf(",\n")
					}
				}
				fmt.Printf("\nWHERE\n")
				printWhere(cols, oldValues)
			case loader.DeleteDMLType:
				fmt.Println("# DELETE")
				fmt.Printf("# values: %v\n", dml.Values)
				fmt.Printf("DELETE FROM %s WHERE\n", dml.TableName())
				printWhere(cols, values)
			case loader.UnknownDMLType:
				return errors.Errorf("invalid DML type %d", dml.Tp)
			}
			fmt.Println() // newline
		}
	}

	return nil
}

func printWhere(cols []string, values []interface{}) {
	for i, col := range cols {
		if values[i] == nil {
			fmt.Printf("  %s IS NULL", loader.QuoteName(col))
		} else {
			fmt.Printf("  %s = %v", loader.QuoteName(col), values[i])
		}
		if i < len(cols)-1 {
			fmt.Printf(" AND \n")
		} else {
			fmt.Println()
		}
	}
}
