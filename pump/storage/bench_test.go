package storage

import (
	"context"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	pb "github.com/pingcap/tipb/go-binlog"
)

func BenchmarkWriteSync128B(b *testing.B) {
	benchmarkWrite(b, 128, 100, true)
}

func BenchmarkWriteSync1K(b *testing.B) {
	benchmarkWrite(b, 1024, 100, true)
}

func BenchmarkWriteSync10K(b *testing.B) {
	benchmarkWrite(b, 10*1024, 100, true)
}

func BenchmarkWriteSync100K(b *testing.B) {
	benchmarkWrite(b, 100*1024, 100, true)
}

func BenchmarkWriteNoSync128B(b *testing.B) {
	benchmarkWrite(b, 128, 100, false)
}

func BenchmarkWriteNoSync1K(b *testing.B) {
	benchmarkWrite(b, 1024, 100, false)
}

func BenchmarkWriteNoSync10K(b *testing.B) {
	benchmarkWrite(b, 100*1024, 100, false)
}

func BenchmarkWriteNoSync100K(b *testing.B) {
	benchmarkWrite(b, 100*1024, 100, false)
}

func BenchmarkPull128B(b *testing.B) {
	benchmarkPull(b, 128, b.N)
}

func BenchmarkPull1K(b *testing.B) {
	benchmarkPull(b, 1024, b.N)
}

func BenchmarkPull10K(b *testing.B) {
	benchmarkPull(b, 10*1024, b.N)
}

func BenchmarkPull100K(b *testing.B) {
	benchmarkPull(b, 100*1024, b.N)
}

func benchmarkPull(b *testing.B, prewriteValueSize int, binlogNum int) {
	append := newAppend(b)
	defer os.RemoveAll(append.dir)

	// populate data
	prewriteValue := make([]byte, prewriteValueSize)
	var ts int64
	getTS := func() int64 {
		return atomic.AddInt64(&ts, 1)
	}

	var wg sync.WaitGroup
	for i := 0; i < binlogNum; i++ {
		wg.Add(1)
		func() {
			defer wg.Done()
			// write P binlog
			binlog := new(pb.Binlog)
			binlog.Tp = pb.BinlogType_Prewrite
			startTs := getTS()
			binlog.StartTs = startTs
			binlog.PrewriteValue = prewriteValue

			err := append.WriteBinlog(binlog)
			if err != nil {
				b.Fatal(err)
			}

			// write C binlog
			binlog = new(pb.Binlog)
			binlog.Tp = pb.BinlogType_Commit
			binlog.StartTs = startTs
			binlog.CommitTs = getTS()
			err = append.WriteBinlog(binlog)
			if err != nil {
				b.Fatal(err)
			}
		}()
	}

	// wait finish populate data
	wg.Wait()

	runtime.GC()
	b.ResetTimer()

	pulller := append.PullCommitBinlog(context.Background(), 0)

	cnt := 0
	for b := range pulller {
		_ = b
		cnt++
		if cnt == binlogNum {
			break
		}
	}

	if cnt != binlogNum {
		b.Fatalf("only get %d binlog, should has %d", cnt, binlogNum)
	}

	// just count the prewriteValueSize
	b.SetBytes(int64(prewriteValueSize))
}

func benchmarkWrite(b *testing.B, prewriteValueSize int, parallelism int, sync bool) {
	b.SetParallelism(parallelism)

	prewriteValue := make([]byte, prewriteValueSize)

	append := newAppendWithOptions(b, DefaultOptions().WithSync(sync))
	defer os.RemoveAll(append.dir)

	b.ResetTimer()

	var ts int64
	getTS := func() int64 {
		return atomic.AddInt64(&ts, 1)
	}
	b.RunParallel(func(pbench *testing.PB) {
		for pbench.Next() {
			// write P binlog
			binlog := new(pb.Binlog)
			binlog.Tp = pb.BinlogType_Prewrite
			startTs := getTS()
			binlog.StartTs = startTs
			binlog.PrewriteValue = prewriteValue

			err := append.WriteBinlog(binlog)
			if err != nil {
				b.Fatal(err)
			}

			// write C binlog
			binlog = new(pb.Binlog)
			binlog.Tp = pb.BinlogType_Commit
			binlog.StartTs = startTs
			binlog.CommitTs = getTS()
			err = append.WriteBinlog(binlog)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// just count the prewriteValueSize
	b.SetBytes(int64(prewriteValueSize))
}
