package drainer

import (
	"io/ioutil"
	"os"

	. "github.com/pingcap/check"
)

var _ = Suite(&testServerSuite{})

type testServerSuite struct{}

func (t *testServerSuite) TestNewServerShouldReturnErr(c *C) {
	tmpfile, err := ioutil.TempFile("", "test")
	if err != nil {
		c.Fatal("Failed to create temp file.")
	}
	defer os.Remove(tmpfile.Name())

	cfg := NewConfig()
	cfg.DataDir = tmpfile.Name()
	cfg.ListenAddr = "http://" + cfg.ListenAddr
	_, err = NewServer(cfg)
	c.Assert(err, ErrorMatches, ".*mkdir.*")
}
