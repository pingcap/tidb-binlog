package storage

import (
	"math/rand"
	"os"
	"path"
	"strconv"
	"time"

	fuzz "github.com/google/gofuzz"
	"github.com/ngaut/log"
	"github.com/pingcap/check"
	pb "github.com/pingcap/tipb/go-binlog"
)

func init() {
	rand.Seed(time.Now().Unix())
	log.SetLevel(log.LOG_LEVEL_ERROR)
}

type VlogSuit struct{}

var _ = check.Suite(VlogSuit{})

func randRequest() *request {
	var payload []byte
	var ts int64
	f := fuzz.New().NumElements(1, 20)
	f.Fuzz(&ts)
	f.Fuzz(&payload)
	return &request{
		startTS: ts,
		payload: payload,
		tp:      pb.BinlogType_Prewrite,
	}
}

func newVlog(c *check.C) *valueLog {
	return newVlogWithOptions(c, DefaultOptions())
}

func newVlogWithOptions(c *check.C, options *Options) *valueLog {
	var err error

	dir := path.Join(os.TempDir(), strconv.Itoa(rand.Int()))
	c.Log("use dir: ", dir)
	err = os.Mkdir(dir, 0777)
	c.Assert(err, check.IsNil)

	vlog := new(valueLog)
	err = vlog.open(dir, options)
	c.Assert(err, check.IsNil)

	return vlog
}

func (vs *VlogSuit) TestOpenEmpty(c *check.C) {
	vlog := newVlog(c)
	defer os.RemoveAll(vlog.dirPath)
}

func (vs *VlogSuit) TestSingleWriteRead(c *check.C) {
	vlog := newVlog(c)
	defer os.RemoveAll(vlog.dirPath)

	req := randRequest()
	err := vlog.write([]*request{req})
	c.Assert(err, check.IsNil)

	payload, err := vlog.readValue(req.valuePointer)
	c.Assert(err, check.IsNil)

	c.Assert(req.payload, check.DeepEquals, payload, check.Commentf("data read back not equal"))
}

func (vs *VlogSuit) TestBatchWriteRead(c *check.C) {
	testBatchWriteRead(c, 1, DefaultOptions())

	testBatchWriteRead(c, 1024, DefaultOptions())

	// set small valueLogFileSize, so we can test multi log file case
	testBatchWriteRead(c, 4096, DefaultOptions().WithValueLogFileSize(500))
}

func testBatchWriteRead(c *check.C, reqNum int, options *Options) {
	vlog := newVlogWithOptions(c, options)
	defer os.RemoveAll(vlog.dirPath)

	n := reqNum
	var reqs []*request
	for i := 0; i < n; i++ {
		reqs = append(reqs, randRequest())
	}

	err := vlog.write(reqs)
	c.Assert(err, check.IsNil)

	for _, req := range reqs {
		payload, err := vlog.readValue(req.valuePointer)
		c.Assert(err, check.IsNil)

		c.Assert(req.payload, check.DeepEquals, payload, check.Commentf("data read back not equal"))
	}

	// test scan start at the middle point of request
	idx := len(reqs) / 2
	vlog.scan(reqs[idx].valuePointer, func(vp valuePointer, record *Record) error {
		c.Assert(record.payload, check.DeepEquals, reqs[idx].payload, check.Commentf("data read back not equal"))
		idx++
		return nil
	})
}

func (vs *VlogSuit) TestCloseAndOpen(c *check.C) {
	vlog := newVlog(c)
	defer os.RemoveAll(vlog.dirPath)

	n := 100
	var reqs []*request
	for i := 0; i < n; i++ {
		// close and open back every time
		var err error
		err = vlog.close()
		c.Assert(err, check.IsNil)

		err = vlog.open(vlog.dirPath, vlog.opt)
		c.Assert(err, check.IsNil)

		req := randRequest()
		reqs = append(reqs, req)
		err = vlog.write([]*request{req})
		c.Assert(err, check.IsNil)
	}

	for _, req := range reqs {
		payload, err := vlog.readValue(req.valuePointer)
		c.Assert(err, check.IsNil)

		c.Assert(req.payload, check.DeepEquals, payload, check.Commentf("data read back not equal"))
	}

}

func (vs *VlogSuit) TestGCTS(c *check.C) {
	vlog := newVlog(c)
	defer os.RemoveAll(vlog.dirPath)

	var pointers []valuePointer
	// write 100 * 10 = 1000M
	for i := 0; i < 100; i++ {
		req := &request{
			startTS: int64(i),
			tp:      pb.BinlogType_Prewrite,
			payload: make([]byte, 10*(1<<20)),
		}
		err := vlog.write([]*request{req})
		c.Assert(err, check.IsNil)
		pointers = append(pointers, req.valuePointer)
	}

	before := len(vlog.filesMap)
	c.Logf("before log file num: %d", before)
	vlog.gcTS(90)
	after := len(vlog.filesMap)
	c.Logf("after log file num: %d", after)

	c.Assert(after, check.Less, before, check.Commentf("no file is deleted"))

	var err error
	// ts 0 has been gc
	_, err = vlog.readValue(pointers[0])
	c.Assert(err, check.IsNil)

	// ts 91 should not be gc
	_, err = vlog.readValue(pointers[91])
	c.Assert(err, check.IsNil)
}

type ValuePointerSuite struct {
}

var _ = check.Suite(&ValuePointerSuite{})

func (vps *ValuePointerSuite) TestValuePointerMarshalBinary(c *check.C) {
	var vp valuePointer
	fuzz := fuzz.New()
	fuzz.Fuzz(&vp)

	var expect valuePointer
	data, err := vp.MarshalBinary()
	c.Assert(err, check.IsNil)

	err = expect.UnmarshalBinary(data)
	c.Assert(err, check.IsNil)

	c.Assert(vp, check.Equals, expect)
}
