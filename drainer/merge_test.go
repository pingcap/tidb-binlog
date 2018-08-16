package drainer

import (
	"strconv"
	"sync"
	"time"

	. "github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
)

var _ = Suite(&testMergerSuite{})

type testMergerSuite struct{}

func (s *testMergerSuite) TestMerge(c *C) {
	s.merge(c, "normal")
	s.merge(c, "heap")
}

func (s *testMergerSuite) merge(c *C, strategy string) {
	var ts []int64
	var outputTs []int64
	var l sync.Mutex
	var maxTS int64

	binlogNum := 9
	sourceNum := 5
	sources := make([]MergeSource, sourceNum)
	for i := 0; i < sourceNum; i++ {
		source := MergeSource{
			ID:     strconv.Itoa(i),
			Source: make(chan MergeItem),
		}
		sources[i] = source
	}
	merger := NewMerger(0, strategy, sources...)

	// get output from merger
	go func() {
		for {
			timeout := time.After(time.Second * 5)
			select {
			case item, ok := <-merger.Output():
				if ok {
					outputTs = append(outputTs, item.GetCommitTs())
				} else {
					return
				}
			case <-timeout:
				c.Fatal("timeout to consume merger output")
			}
		}
	}()

	var wg sync.WaitGroup
	// + 1 because we add a new source later
	wg.Add(sourceNum + 1)

	// generate binlog for the sources in merger
	for id := range sources {
		go func(id int) {
			for j := 0; j < binlogNum; j++ {
				binlog := new(pb.Binlog)
				binlog.CommitTs = int64(j*100 + id)
				binlogItem := newBinlogItem(binlog, strconv.Itoa(id))
				sources[id].Source <- binlogItem
				l.Lock()
				ts = append(ts, binlog.CommitTs)
				if binlog.CommitTs > maxTS {
					maxTS = binlog.CommitTs
				}
				l.Unlock()

			}
			wg.Done()
		}(id)
	}

	// add new source
	source := MergeSource{
		ID:     strconv.Itoa(sourceNum),
		Source: make(chan MergeItem),
	}
	merger.AddSource(source)

	// write binlog to new source
	go func() {
		l.Lock()
		baseTS := maxTS / 10 * 10
		l.Unlock()
		for j := 0; j < binlogNum; j++ {
			binlog := new(pb.Binlog)
			binlog.CommitTs = baseTS + int64(j*100+sourceNum)
			binlogItem := newBinlogItem(binlog, strconv.Itoa(sourceNum))
			source.Source <- binlogItem
			l.Lock()
			ts = append(ts, binlog.CommitTs)
			if binlog.CommitTs > maxTS {
				maxTS = binlog.CommitTs
			}
			l.Unlock()
		}
		wg.Done()
	}()

	// add wrong binlog to the first source
	l.Lock()
	currentMaxTS := maxTS
	l.Unlock()
	binlog := new(pb.Binlog)
	binlog.CommitTs = currentMaxTS - 1
	sources[0].Source <- newBinlogItem(binlog, "0")
	l.Lock()
	ts = append(ts, binlog.CommitTs)
	l.Unlock()

	wg.Wait()
	var currentTs int64
	for _, ts := range outputTs {
		c.Assert(ts > currentTs, Equals, true)
		currentTs = ts
	}
}
