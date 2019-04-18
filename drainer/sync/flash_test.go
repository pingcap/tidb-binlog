package sync

import (
	"database/sql"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/drainer/translator"
)

var _ = check.Suite(&flashSuite{})

type flashSuite struct {
	flash *FlashSyncer

	flashMock sqlmock.Sqlmock
	gen       *translator.BinlogGenrator
}

func (s *flashSuite) setUpTest(c *check.C, timeLimit string, sizeLimit string) {
	s.gen = &translator.BinlogGenrator{}

	// create flash syncer and mock
	oldOpenCH := openCH
	defer func() {
		openCH = oldOpenCH
	}()
	openCH = func(string, int, string, string, string, int) (db *sql.DB, err error) {
		db, s.flashMock, err = sqlmock.New()
		return
	}

	cfg := &DBConfig{
		Host:      "localhost:3306",
		TimeLimit: timeLimit,
		SizeLimit: sizeLimit,
	}

	var err error
	s.flash, err = NewFlashSyncer(cfg, s.gen)
	c.Assert(err, check.IsNil)
}

func (s *flashSuite) TestFlushBySizeLimit(c *check.C) {
	s.setUpTest(c, "1h", "1")

	gen := s.gen
	syncer := s.flash

	gen.SetInsert(c)
	item := &Item{
		Binlog:        gen.TiBinlog,
		PrewriteValue: gen.PV,
		Schema:        gen.Schema,
		Table:         gen.Table,
	}

	// set up mock
	s.flashMock.ExpectBegin()
	prepare := s.flashMock.ExpectPrepare("IMPORT INTO .*")
	prepare.ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
	s.flashMock.ExpectCommit()

	err := syncer.Sync(item)
	c.Assert(err, check.IsNil)

	err = s.flashMock.ExpectationsWereMet()
	c.Assert(err, check.IsNil)
}

func (s *flashSuite) TestFlushByTimeLimit(c *check.C) {
	s.setUpTest(c, "1s", "100000000")

	gen := s.gen
	syncer := s.flash

	gen.SetInsert(c)
	item := &Item{
		Binlog:        gen.TiBinlog,
		PrewriteValue: gen.PV,
		Schema:        gen.Schema,
		Table:         gen.Table,
	}

	// set up mock
	s.flashMock.ExpectBegin()
	prepare := s.flashMock.ExpectPrepare("IMPORT INTO .*")
	prepare.ExpectExec().WillReturnResult(sqlmock.NewResult(0, 1))
	s.flashMock.ExpectCommit()

	err := syncer.Sync(item)
	c.Assert(err, check.IsNil)

	select {
	case <-syncer.Successes():
		break
	case <-time.After(time.Second * 5):
		c.Fatal("time out to get from Successes")
	}
}

func (s *flashSuite) TearDownTest(c *check.C) {
	s.flashMock.ExpectClose()

	s.flash.Close()
}
