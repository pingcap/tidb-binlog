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
package main

import (
	"encoding/binary"
	"flag"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-binlog/pump/storage"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/syndtr/goleveldb/leveldb"
	"go.uber.org/zap"
)

var tsKeyPrefix = []byte("ts:")

func main() {
	// Parse args
	kvDir := flag.String("kv_dir", "", "path of the pump kv directory")
	repair := flag.Bool("repair", false, "repair the offsets in LevelDB")
	flag.Parse()
	path := flag.Arg(0)

	// Open log file
	log.Info("Target vlog", zap.String("path", path))
	fid, err := storage.ParseFid(path)
	if err != nil {
		log.Fatal("Failed to parse fid", zap.Error(err), zap.String("path", path))
	}
	log.Info("Parse fid", zap.Uint32("fid", fid))
	logFile, err := storage.NewLogFile(fid, path)
	if err != nil {
		log.Fatal("Failed to open log", zap.Error(err), zap.String("path", path))
	}
	log.Info("Log opened")

	// Open LevelDB
	metadb, err := storage.OpenMetadataDB(*kvDir, nil)
	if err != nil {
		log.Fatal("Failed to open LevelDB", zap.String("path", *kvDir), zap.Error(err))
	}
	defer func() {
		err := metadb.Close()
		if err != nil {
			log.Error("Failed to close metadb", zap.Error(err))
		}
	}()
	log.Info("LevelDB opened")

	var corruptionPos int64 = -1
	checked := make(map[int64]struct{}, 64)
	logFile.Scan(
		0,
		func(vp storage.ValuePointer, record *storage.Record) error {
			bl, err := record.GetBinlog()
			if err != nil {
				log.Fatal("Failed to get binlog", zap.Int64("offset", vp.Offset), zap.Error(err))
			}
			// Ignore binlogs with zero CommitTs, eg. rollback binlogs
			if bl.Tp == pb.BinlogType_Rollback && bl.CommitTs == 0 {
				return nil
			}
			tso := getTSO(bl)
			if _, ok := checked[tso]; ok {
				log.Info("Skip duplicate binlog", zap.Int64("tso", tso))
				return nil
			}
			checked[tso] = struct{}{}
			key := encodeKey(bl)

			pointer, err := getSavedPointer(metadb, key)
			if err != nil {
				log.Error("Failed to get saved data", zap.Error(err), zap.Binary("key", key))
				return nil
			}

			if pointer.Fid != fid {
				// Reading a different Fid, this should never happen
				log.Fatal("Fid mismatch", zap.Uint32("get", pointer.Fid), zap.Uint32("expected", fid))
			} else if pointer.Offset != vp.Offset {
				log.Info(
					"Pointer offset mismatch",
					zap.Int64("get", pointer.Offset),
					zap.Int64("expected", vp.Offset),
					zap.Int64("diff", vp.Offset-pointer.Offset),
				)
				if *repair {
					data, err := vp.MarshalBinary()
					if err != nil {
						log.Fatal("Failed to marshal pointer", zap.Error(err), zap.Int64("offset", vp.Offset))
					}
					if err := metadb.Put(key, data, nil); err != nil {
						log.Fatal("Failed to repair pointer", zap.Error(err), zap.Int64("offset", vp.Offset))
					}
					log.Info("Fixed", zap.Int64("offset", vp.Offset), zap.Binary("key", key))
					return nil
				}
			}
			return nil
		},
		func(offset int64, length int, err error) {
			log.Info("Corruption detected",
				zap.Int64("offset", offset), zap.Int("len", length), zap.Error(err))
			corruptionPos = offset
		},
	)
	if corruptionPos == -1 {
		log.Info("Congratulations! No corruption.")
	}
}

func getTSO(bl *pb.Binlog) int64 {
	if bl.Tp == pb.BinlogType_Prewrite {
		return bl.StartTs
	} else {
		return bl.CommitTs
	}
}

func encodeKey(bl *pb.Binlog) []byte {
	ts := getTSO(bl)

	buf := make([]byte, 8+len(tsKeyPrefix))
	copy(buf, tsKeyPrefix)

	b := buf[len(tsKeyPrefix):]
	binary.BigEndian.PutUint64(b, uint64(ts))

	return buf
}

func getSavedPointer(db *leveldb.DB, key []byte) (*storage.ValuePointer, error) {
	pointerData, err := db.Get(key, nil)
	if err != nil {
		return nil, errors.Annotatef(err, "get from leveldb key: %v", key)
	}
	var pointer storage.ValuePointer
	err = pointer.UnmarshalBinary(pointerData)
	if err != nil {
		return nil, errors.Annotate(err, "unmarshal data")
	}
	return &pointer, nil
}
