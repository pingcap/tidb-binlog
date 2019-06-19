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
		log.Fatal("Failed to  parse fid", zap.Error(err), zap.String("path", path))
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
	log.Info("LevelDB opened")

	var corruptionPos int64 = -1
	logFile.Scan(
		0,
		func(vp storage.ValuePointer, record *storage.Record) error {
			// We are only interested in records after the corruption position
			if corruptionPos == -1 {
				return nil
			}
			bl, err := record.GetBinlog()
			if err != nil {
				log.Fatal("Failed to get binlog", zap.Int64("offset", vp.Offset), zap.Error(err))
			}
			key := encodeKey(bl)

			if *repair {
				data, err := vp.MarshalBinary()
				if err != nil {
					log.Fatal("Failed to marshal pointer", zap.Error(err), zap.Int64("offset", vp.Offset))
				}
				if err := metadb.Put(key, data, nil); err != nil {
					log.Fatal("Failed to repair pointer", zap.Error(err), zap.Int64("offset", vp.Offset))
				}
				log.Info("Fixed", zap.Int64("offset", vp.Offset), zap.ByteString("key", key))
				return nil
			}

			pointer, err := getSavedPointer(metadb, key)
			if err != nil {
				log.Error("Failed to get saved data", zap.Error(err), zap.ByteString("key", key))
				return nil
			}

			if pointer.Fid != vp.Fid || pointer.Offset != vp.Offset {
				log.Info("Pointer mismatch detected", zap.Reflect("get", pointer), zap.Reflect("expected", vp))
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

func encodeKey(bl *pb.Binlog) []byte {
	var ts int64
	if bl.Tp == pb.BinlogType_Prewrite {
		ts = bl.StartTs
	} else {
		ts = bl.CommitTs
	}

	buf := make([]byte, 8+len(tsKeyPrefix))
	copy(buf, tsKeyPrefix)

	b := buf[len(tsKeyPrefix):]
	binary.BigEndian.PutUint64(b, uint64(ts))

	return buf
}

func getSavedPointer(db *leveldb.DB, key []byte) (*storage.ValuePointer, error) {
	pointerData, err := db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			log.Fatal(
				"Key not found, please make sure the kv directory is correct",
				zap.ByteString("key", key),
			)
		}
		return nil, errors.Annotate(err, "get from leveldb")
	}
	var pointer storage.ValuePointer
	err = pointer.UnmarshalBinary(pointerData)
	if err != nil {
		return nil, errors.Annotate(err, "unmarshal data")
	}
	return &pointer, nil
}
