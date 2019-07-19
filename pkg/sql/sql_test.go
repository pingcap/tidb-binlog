// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package sql

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

func Test(t *testing.T) { TestingT(t) }

type quoteSuite struct{}

var _ = Suite(&quoteSuite{})

func (s *quoteSuite) TestQuoteSchema(c *C) {
	c.Assert(QuoteSchema("music", "subjects"), Equals, "`music`.`subjects`")
	c.Assert(QuoteSchema("wEi`rd", "Na`me"), Equals, "`wEi``rd`.`Na``me`")
}

type parseCHAddrSuite struct{}

var _ = Suite(&parseCHAddrSuite{})

func (s *parseCHAddrSuite) TestShouldRetErrForInvalidAddr(c *C) {
	_, err := ParseCHAddr("localhost:8080,localhost:8X8X")
	c.Assert(err, NotNil)
	_, err = ParseCHAddr("asdfasdf")
	c.Assert(err, NotNil)
}

func (s *parseCHAddrSuite) TestShouldRetHostAndPort(c *C) {
	addrs, err := ParseCHAddr("localhost:8080,test2:8081")
	c.Assert(err, IsNil)
	c.Assert(addrs, HasLen, 2)
	c.Assert(addrs[0].Host, Equals, "localhost")
	c.Assert(addrs[0].Port, Equals, 8080)
	c.Assert(addrs[1].Host, Equals, "test2")
	c.Assert(addrs[1].Port, Equals, 8081)
}

type composeCHDSNSuite struct{}

var _ = Suite(&composeCHDSNSuite{})

func (s *composeCHDSNSuite) TestShouldIncludeAllInfo(c *C) {
	dbDSN := composeCHDSN("test_node1", 7979, "root", "secret", "test", 1024)
	c.Assert(dbDSN, Equals, "tcp://test_node1:7979?username=root&password=secret&database=test&block_size=1024&")
	dbDSN = composeCHDSN("test", 7979, "root", "", "test", -1)
	c.Assert(dbDSN, Equals, "tcp://test:7979?username=root&database=test&")
}

type SQLErrSuite struct{}

var _ = Suite(&SQLErrSuite{})

func (s *SQLErrSuite) TestGetSQLErrCode(c *C) {
	_, ok := GetSQLErrCode(errors.New("test"))
	c.Assert(ok, IsFalse)
	_, ok = GetSQLErrCode(&mysql.MySQLError{Number: 1146})
	c.Assert(ok, IsTrue)
}

func (s *SQLErrSuite) TestIgnoreDDLError(c *C) {
	c.Assert(IgnoreDDLError(&mysql.MySQLError{Number: 1146}), IsTrue)
	c.Assert(IgnoreDDLError(&mysql.MySQLError{Number: 1032}), IsFalse)
	c.Assert(IgnoreDDLError(&mysql.MySQLError{Number: 1176}), IsTrue)
}

type sqlSuite struct {
	db               *sql.DB
	mock             sqlmock.Sqlmock
	oldRetryWaitTime time.Duration
}

var _ = Suite(&sqlSuite{})

func (s *sqlSuite) SetUpTest(c *C) {
	var err error
	s.db, s.mock, err = sqlmock.New()
	c.Assert(err, IsNil)

	s.oldRetryWaitTime = RetryWaitTime
	RetryWaitTime = 10 * time.Millisecond
}

func (s *sqlSuite) TearDownTest(c *C) {
	RetryWaitTime = s.oldRetryWaitTime

	c.Assert(s.mock.ExpectationsWereMet(), IsNil)
	s.db.Close()
}

func (s *sqlSuite) TestGetTidbPosition(c *C) {
	s.mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(
		sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
			AddRow("tidb-binlog", uint64(407774332609932), "", "", ""),
	)

	tso, err := GetTidbPosition(s.db)
	c.Assert(err, IsNil)
	c.Assert(tso, Equals, int64(407774332609932))
}

const (
	testQuery1 = "UPDATE foo SET bar = bar - ?"
	testQuery2 = "DELETE FROM foo WHERE bar <= ?"
)

func (s *sqlSuite) TestExecuteTxnSuccess(c *C) {
	var (
		delay1 time.Duration = 201
		delay2 time.Duration = 101
	)
	s.mock.ExpectBegin()
	s.mock.ExpectExec(testQuery1).
		WithArgs(1).
		WillDelayFor(time.Millisecond * delay1).
		WillReturnResult(sqlmock.NewResult(18, 102))
	s.mock.ExpectExec(testQuery2).
		WithArgs(0).
		WillDelayFor(time.Millisecond * delay2).
		WillReturnResult(sqlmock.NewResult(19, 63))
	s.mock.ExpectCommit()

	startTime := time.Now()
	histogram := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Buckets: prometheus.LinearBuckets(0, 0.1, 5)},
		[]string{"exec"},
	)
	histogramFix := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Buckets: prometheus.LinearBuckets(0, 0.1, 5)},
		[]string{"execFix"},
	)
	err := ExecuteTxnWithHistogram(
		s.db,
		[]string{testQuery1, testQuery2},
		[][]interface{}{{1}, {0}},
		histogram,
	)
	c.Assert(err, IsNil)
	diffDuration := time.Since(startTime.Add(time.Millisecond * time.Duration((delay1 + delay2))))

	// there exists some code running between db exec and prometheus histogram observation,
	// so we calcuate the diff duration to make the sample_sum check more accurate.
	var metricFix io_prometheus_client.Metric
	histogramFix.WithLabelValues("execFix").Observe((time.Millisecond * delay1).Seconds())
	histogramFix.WithLabelValues("execFix").Observe((time.Millisecond*delay2 + diffDuration).Seconds())
	err = histogramFix.WithLabelValues("execFix").(prometheus.Metric).Write(&metricFix)
	c.Assert(err, IsNil)
	upperBound := metricFix.Histogram.GetSampleSum()

	// extract the content of the histogram.
	var metric io_prometheus_client.Metric
	err = histogram.WithLabelValues("exec").(prometheus.Metric).Write(&metric)
	c.Assert(err, IsNil)
	c.Assert(metric.Histogram.GetSampleCount(), Equals, uint64(2))
	sum := metric.Histogram.GetSampleSum()
	c.Assert(sum, Greater, 0.302)
	c.Assert(sum, Less, upperBound)
	buckets := metric.Histogram.GetBucket()
	c.Assert(buckets, HasLen, 5)
	c.Logf("buckets = %q", buckets)
	c.Assert(buckets[0].GetCumulativeCount(), Equals, uint64(0)) // <= 0ms
	c.Assert(buckets[1].GetCumulativeCount(), Equals, uint64(0)) // <= 100ms
	c.Assert(buckets[2].GetCumulativeCount(), Equals, uint64(1)) // <= 200ms
	c.Assert(buckets[3].GetCumulativeCount(), Equals, uint64(2)) // <= 300ms
	c.Assert(buckets[4].GetCumulativeCount(), Equals, uint64(2)) // <= infinity
}

func (s *sqlSuite) TestExecuteTxnRollback(c *C) {
	s.mock.ExpectBegin()
	s.mock.ExpectExec(testQuery1).
		WithArgs(1).
		WillReturnError(errors.New("force txn rollback error"))
	s.mock.ExpectRollback()

	err := ExecuteTxn(s.db, []string{testQuery1, testQuery2}, [][]interface{}{{1}, {0}})
	c.Assert(err, ErrorMatches, "force txn rollback error")
}

func (s *sqlSuite) TestExecuteSQLsWithRetry(c *C) {
	s.mock.ExpectBegin().WillReturnError(errors.New("force retry #0"))

	s.mock.ExpectBegin()
	s.mock.ExpectExec(testQuery1).
		WithArgs(1).
		WillReturnError(errors.New("force retry #1"))
	s.mock.ExpectRollback()

	s.mock.ExpectBegin()
	s.mock.ExpectExec(testQuery1).
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(18, 102))
	s.mock.ExpectExec(testQuery2).
		WithArgs(0).
		WillReturnError(errors.New("force retry #2"))
	s.mock.ExpectRollback()

	s.mock.ExpectBegin()
	s.mock.ExpectExec(testQuery1).
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(19, 102))
	s.mock.ExpectExec(testQuery2).
		WithArgs(0).
		WillReturnResult(sqlmock.NewResult(20, 63))
	s.mock.ExpectCommit().WillReturnError(errors.New("force retry #3"))

	s.mock.ExpectBegin()
	s.mock.ExpectExec(testQuery1).
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(21, 102))
	s.mock.ExpectExec(testQuery2).
		WithArgs(0).
		WillReturnResult(sqlmock.NewResult(22, 63))
	s.mock.ExpectCommit()

	err := ExecuteSQLs(s.db, []string{testQuery1, testQuery2}, [][]interface{}{{1}, {0}}, false)
	c.Assert(err, IsNil)
}

func (s *sqlSuite) TestExecuteSQLsWithRetryAndFailure(c *C) {
	query := "PERFORM SOME INVALID QUERY"

	for i := 0; i < MaxDDLRetryCount; i++ {
		s.mock.ExpectBegin()
		s.mock.ExpectExec(query).WillReturnError(errors.New("syntax error"))
		s.mock.ExpectRollback()
	}

	err := ExecuteSQLs(s.db, []string{query}, [][]interface{}{nil}, true)
	c.Assert(err, ErrorMatches, "syntax error")
}
