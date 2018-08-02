package drainer

 import (
 	"sort"
 	"strconv"
 	"sync"
 	"time"

 	fuzz "github.com/google/gofuzz"
 	. "github.com/pingcap/check"
 	pb "github.com/pingcap/tipb/go-binlog"
 )

 var _ = Suite(&testMergerSuite{})

 type testMergerSuite struct{}

 func (s *testMergerSuite) TestMerge(c *C) {
 	sourceNum := 10

 	var ts []int
 	fuzz := fuzz.New().NumElements(sourceNum*10, sourceNum*10).NilChance(0)
 	fuzz.Fuzz(&ts)
 	sort.Ints(ts)

 	c.Log("ts: ", ts)

 	sources := make([]MergeSource, sourceNum)
 	for i := 0; i < sourceNum; i++ {
 		source := MergeSource{
 			ID:     strconv.Itoa(i),
 			Source: make(chan MergeItem),
 		}
 		sources[i] = source
 	}

 	var wg sync.WaitGroup
 	wg.Add(sourceNum)
 	for i := 0; i < sourceNum; i++ {
 		go func(idx int) {
 			for j := idx; j < len(ts); j += sourceNum {
 				binlog := new(pb.Binlog)
 				binlog.CommitTs = int64(ts[j])
 				sources[idx].Source <- binlog
 			}

 			close(sources[idx].Source)
 			wg.Done()
 		}(i)
 	}

 	var outputTs []int
 	merger := NewMerger(sources...)
 	go func() {
 		wg.Wait()

 		// add a source and push one binlog
 		source := MergeSource{
 			ID:     "addid",
 			Source: make(chan MergeItem),
 		}
 		merger.AddSource(source)

 		last := ts[len(ts)-1]
 		ts = append(ts, last+1)
 		binlog := new(pb.Binlog)
 		binlog.CommitTs = int64(last + 1)
 		source.Source <- binlog
 		close(source.Source)

 		merger.Close()
 	}()

 L:
 	for {
 		timeout := time.After(time.Second * 5)
 		select {
 		case item, ok := <-merger.Output():
 			if ok {
 				binlog := item.(*pb.Binlog)
 				outputTs = append(outputTs, int(binlog.CommitTs))
 			} else {
 				break L
 			}
 		case <-timeout:
 			c.Fatal("timeout to consume merger output")
 		}
 	}

 	c.Assert(ts, DeepEquals, outputTs)
 }