package cistern

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tipb/go-binlog"
)

func (s *testCisternSuite) TestComparePos(c *C) {
	c.Assert(ComparePos(binlog.Pos{Suffix: 2, Offset: 3}, binlog.Pos{Suffix: 3, Offset: 4}), Equals, -1)
	c.Assert(ComparePos(binlog.Pos{Suffix: 4, Offset: 3}, binlog.Pos{Suffix: 3, Offset: 4}), Equals, 1)
	c.Assert(ComparePos(binlog.Pos{Suffix: 3, Offset: 2}, binlog.Pos{Suffix: 3, Offset: 4}), Equals, -1)
	c.Assert(ComparePos(binlog.Pos{Suffix: 3, Offset: 5}, binlog.Pos{Suffix: 3, Offset: 4}), Equals, 1)
	c.Assert(ComparePos(binlog.Pos{Suffix: 3, Offset: 4}, binlog.Pos{Suffix: 3, Offset: 4}), Equals, 0)
}

func (s *testCisternSuite) TestCalculateNextPos(c *C) {
	entry := binlog.Entity{
		Payload: []byte("test"),
		Pos:     binlog.Pos{Suffix: 1, Offset: 3},
	}

	p := CalculateNextPos(entry)
	c.Assert(p.Offset, Equals, int64(23))
}

func (s *testCisternSuite) TestPayload(c *C) {
	payload, err := encodePayload([]byte("test"))
	c.Assert(err, IsNil)
	c.Assert(payload, HasLen, 19)

	data, _, err := decodePayload(payload)
	c.Assert(err, IsNil)
	c.Assert(data, DeepEquals, []byte("test"))
}

func (s *testCisternSuite) TestPosToFloat(c *C) {
	pos := binlog.Pos{
		Suffix: 4,
		Offset: 3721,
	}
	f := posToFloat(&pos)
	c.Assert(f, Equals, 3721.4)
}

func (s *testCisternSuite) TestDecodeJob(c *C) {
	dbName := model.NewCIStr("Test")
	tbName := model.NewCIStr("T")
	tblInfo := &model.TableInfo{
		ID:    2,
		Name:  tbName,
		State: model.StatePublic,
	}

	dbInfo := &model.DBInfo{
		ID:    3,
		Name:  dbName,
		State: model.StatePublic,
	}
	jobs := []*model.Job{
		{
			ID:       5,
			SchemaID: 3,
			Type:     model.ActionCreateSchema,
			Args:     []interface{}{123, dbInfo},
		}, {
			ID:       5,
			SchemaID: 3,
			Type:     model.ActionCreateSchema,
			Args:     []interface{}{123, 123},
		}, {
			ID:       6,
			SchemaID: 3,
			TableID:  2,
			Type:     model.ActionCreateTable,
			Args:     []interface{}{123, tblInfo},
		}, {
			ID:       6,
			SchemaID: 3,
			TableID:  2,
			Type:     model.ActionCreateTable,
			Args:     []interface{}{123, 123},
		}, {
			ID:       6,
			SchemaID: 3,
			TableID:  2,
			Type:     model.ActionType(233),
			Args:     []interface{}{123, 123},
		},
	}

	exceptedErr := []bool{false, true, false, true, true}

	for index := range jobs {
		job := mustTranslateJob(c, jobs[index])
		if exceptedErr[index] {
			c.Assert(decodeJob(job), NotNil)
		} else {
			c.Assert(decodeJob(job), IsNil)
		}
	}
}

func mustTranslateJob(c *C, job *model.Job) *model.Job {
	rawJob, err := job.Encode()
	c.Assert(err, IsNil)
	err = job.Decode(rawJob)
	c.Assert(err, IsNil)
	return job
}
