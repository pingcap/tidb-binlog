package pump

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/siddontang/go/ioutil2"
)

var (
	maxSaveTime = 30 * time.Second
)

// checkPoint is local CheckPoint struct.
type checkPoint struct {
	sync.RWMutex

	name     string
	saveTime time.Time

	Position pb.Pos `toml:"position" json:"position"`
}

// newCheckPoint creates a new checkpoint.
func newCheckPoint(name string) (*checkPoint, error) {
	c := &checkPoint{name: name}
	err := c.load()
	return c, errors.Trace(err)
}

func (c *checkPoint) load() error {
	c.Lock()
	defer c.Unlock()

	file, err := os.Open(c.name)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return errors.Trace(err)
	}
	if os.IsNotExist(errors.Cause(err)) {
		return nil
	}
	defer file.Close()

	_, err = toml.DecodeReader(file, c)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (c *checkPoint) save(pos pb.Pos, force bool) error {
	c.Lock()
	defer c.Unlock()

	c.Position.Suffix = pos.Suffix
	c.Position.Offset = pos.Offset

	if !force && !c.check() {
		return nil
	}

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(c)
	if err != nil {
		log.Errorf("syncer save checkpoint info to file %s err %v", c.name, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	err = ioutil2.WriteFileAtomic(c.name, buf.Bytes(), 0644)
	if err != nil {
		log.Errorf("syncer save checkpoint info to file %s err %v", c.name, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	c.saveTime = time.Now()
	return nil
}

func (c *checkPoint) check() bool {
	return time.Since(c.saveTime) >= maxSaveTime
}

func (c *checkPoint) pos() pb.Pos {
	c.RLock()
	defer c.RUnlock()

	return pb.Pos{
		Suffix: c.Position.Suffix,
		Offset: c.Position.Offset,
	}
}

func (c *checkPoint) String() string {
	pos := c.pos()
	return fmt.Sprintf("binlog positions = %+v", pos)
}
