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
	s.merge(c, normalStrategy)
	s.merge(c, heapStrategy)
}

func (s *testMergerSuite) merge(c *C, strategy string) {
	var ts []int64
	var outputTs []int64
	var l sync.Mutex
	var maxTS int64

	binlogNum := 9
	sourceNum := 5
	sources := createMergeSources(sourceNum, 0)
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

func (s *testMergerSuite) TestRemoveSourceWithPendingLogs(c *C) {
	numSources := 5
	sources := createMergeSources(numSources, binlogChanSize)
	merger := NewMerger(-1, heapStrategy, sources...)

	var wg sync.WaitGroup
	for i, src := range sources {
		wg.Add(1)
		go func(id int, src MergeSource) {
			for j := 0; j < binlogChanSize; j++ {
				binlog := new(pb.Binlog)
				binlog.CommitTs = int64(id + j*numSources)
				binlogItem := newBinlogItem(binlog, src.ID)
				c.Logf("[source %s] adding binlog %d", src.ID, binlog.CommitTs)
				src.Source <- binlogItem
			}
			wg.Done()
		}(i, src)
	}
	merger.Stop()
	c.Log("Merger stopped")
	wg.Wait()
	c.Log("All binlogs written to source channels")

	// binlogChanSize` and the buffer size of the `output` channel are both `10`,
	// so all sources should have pending logs in the channel at this point
	for _, src := range sources[2:] {
		c.Assert(len(src.Source) > 0, Equals, true)
		close(src.Source)
		merger.RemoveSource(src.ID)
	}
	merger.Continue()

	// It takes only one empty source to block the goroutine executing merger.run,
	// due to the way we add our binlogs in this tests, we can't read the last `numSources`
	// binlogs
	numReadableBinlogs := binlogChanSize*numSources - numSources
	output := merger.Output()
	for i := 0; i < numReadableBinlogs; i++ {
		timeout := time.After(time.Second * 3)
		select {
		case item, ok := <-output:
			if !ok {
				c.Fatalf("Merger output channel closed, last read ts: %d", i-1)
			} else {
				c.Logf("Receive %d from merger", item.GetCommitTs())
				c.Assert(item.GetCommitTs(), Equals, int64(i))
			}
		case <-timeout:
			c.Fatal("Timeout consuming merger output")
		}
	}
}

func createMergeSources(sourceNum int, bufferSize int) []MergeSource {
	sources := make([]MergeSource, sourceNum)
	for i := 0; i < sourceNum; i++ {
		source := MergeSource{
			ID:     strconv.Itoa(i),
			Source: make(chan MergeItem, bufferSize),
		}
		sources[i] = source
	}
	return sources
}
