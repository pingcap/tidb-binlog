package storage

import (
	"bytes"
	"math/rand"
	"os"
	"path"
	"reflect"
	"strconv"
	"testing"
	"time"

	fuzz "github.com/google/gofuzz"
	"github.com/ngaut/log"
	pb "github.com/pingcap/tipb/go-binlog"
)

func init() {
	rand.Seed(time.Now().Unix())
	log.SetLevel(log.LOG_LEVEL_ERROR)
}

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

func newVlog(t *testing.T) *valueLog {
	return newVlogWithOptions(t, DefaultOptions())
}

func newVlogWithOptions(t *testing.T, options *Options) *valueLog {
	var err error

	dir := path.Join(os.TempDir(), strconv.Itoa(rand.Int()))
	t.Log("use dir: ", dir)
	err = os.Mkdir(dir, 0777)
	if err != nil {
		t.Error(err)
	}

	vlog := new(valueLog)
	err = vlog.open(dir, options)
	if err != nil {
		t.Error("open fail: ", err)
	}

	return vlog
}

func TestOpenEmpty(t *testing.T) {
	vlog := newVlog(t)
	defer os.RemoveAll(vlog.dirPath)
}

func TestSingleWriteRead(t *testing.T) {
	vlog := newVlog(t)
	defer os.RemoveAll(vlog.dirPath)

	req := randRequest()
	err := vlog.write([]*request{req})
	if err != nil {
		t.Error(err)
	}

	payload, err := vlog.readValue(req.valuePointer)
	if err != nil {
		t.Error(err)
	}

	if bytes.Equal(req.payload, payload) == false {
		t.Errorf("data read back not equal")
	}
}

func TestBatchWriteRead(t *testing.T) {
	testBatchWriteRead(t, 1, DefaultOptions())

	testBatchWriteRead(t, 1024, DefaultOptions())

	// set small valueLogFileSize, so we can test multi log file case
	testBatchWriteRead(t, 4096, DefaultOptions().WithValueLogFileSize(500))
}

func testBatchWriteRead(t *testing.T, reqNum int, options *Options) {
	vlog := newVlogWithOptions(t, options)
	defer os.RemoveAll(vlog.dirPath)

	n := reqNum
	var reqs []*request
	for i := 0; i < n; i++ {
		reqs = append(reqs, randRequest())
	}

	err := vlog.write(reqs)
	if err != nil {
		t.Fatal(err)
	}

	for _, req := range reqs {
		payload, err := vlog.readValue(req.valuePointer)
		if err != nil {
			t.Fatal(err)
		}

		if bytes.Equal(req.payload, payload) == false {
			t.Fatal("data read back not equal")
		}
	}

	// test scan start at the middle point of request
	idx := len(reqs) / 2
	vlog.scan(reqs[idx].valuePointer, func(vp valuePointer, record *Record) error {
		if bytes.Equal(record.payload, reqs[idx].payload) == false {
			t.Fatal("data read back not equal")
		}
		idx++
		return nil
	})
}

func TestCloseAndOpen(t *testing.T) {
	vlog := newVlog(t)
	defer os.RemoveAll(vlog.dirPath)

	n := 100
	var reqs []*request
	for i := 0; i < n; i++ {
		// close and open back every time
		var err error
		err = vlog.close()
		if err != nil {
			t.Error(err)
		}
		err = vlog.open(vlog.dirPath, vlog.opt)
		if err != nil {
			t.Error(err)
		}

		req := randRequest()
		reqs = append(reqs, req)
		err = vlog.write([]*request{req})
		if err != nil {
			t.Error(err)
		}
	}

	for _, req := range reqs {
		payload, err := vlog.readValue(req.valuePointer)
		if err != nil {
			t.Error(err)
		}

		if bytes.Equal(req.payload, payload) == false {
			t.Errorf("data read back not equal")
		}
	}

}

func TestGCTS(t *testing.T) {
	vlog := newVlog(t)
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
		if err != nil {
			t.Error(err)
		}
		pointers = append(pointers, req.valuePointer)
	}

	before := len(vlog.filesMap)
	t.Logf("before log file num: %d", before)
	vlog.gcTS(90)
	after := len(vlog.filesMap)
	t.Logf("after log file num: %d", after)

	if after >= before {
		t.Error("no file is deleted")
	}

	var err error
	// ts 0 has been gc
	_, err = vlog.readValue(pointers[0])
	if err == nil {
		t.Error(err)
	}

	// ts 91 should not be gc
	_, err = vlog.readValue(pointers[91])
	if err != nil {
		t.Error(err)
	}
}

func TestValuePointerMarshalBinary(t *testing.T) {
	var vp valuePointer
	fuzz := fuzz.New()
	fuzz.Fuzz(&vp)

	var expect valuePointer
	data, err := vp.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	err = expect.UnmarshalBinary(data)
	if err != nil {
		t.Fatal(err)
	}

	if reflect.DeepEqual(vp, expect) == false {
		t.Fatalf("want: %v, get: %v\n", vp, expect)
	}
}
