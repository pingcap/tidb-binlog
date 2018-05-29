package dailytest

import (
	"database/sql"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/test/util"
)

// test different data type of mysql
var case1 = []string{`
create table  binlog_case1 (
	id int auto_increment,
	t_bigint BIGINT,
	t_double DOUBLE,
	t_decimal DECIMAL(38,19),
	t_date DATE,
	t_datetime DATETIME,
	t_timestamp TIMESTAMP,
	t_time TIME,
	t_year YEAR,
	t_char CHAR,
	t_varchar VARCHAR(10),
	t_blob BLOB,
	t_text TEXT,
	t_enum ENUM('enum1', 'enum2', 'enum3'),
	

	primary key(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
`,
	`
insert into binlog_case1(t_bigint, t_double, t_decimal
	,t_date, t_datetime, t_timestamp, t_time, t_year
	,t_char, t_varchar, t_blob, t_text, t_enum) values
	(9223372036854775807, 123.123, 123456789012.123456789012
	,'1000-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 1970
	,'测', '测试', 'blob', '测试text', 'enum2');
`,
}

// RunCase run some simple test case
func RunCase(src *sql.DB, dst *sql.DB) {
	err := execSQLs(src, case1)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(15 * time.Second)
	if !util.CheckSyncState(src, dst) {
		log.Fatal("src don't equal dst")
	}
	// clean table
	execSQL(src, "drop table binlog_case1;")
	time.Sleep(15 * time.Second)
	if !util.CheckSyncState(src, dst) {
		log.Fatal("src don't equal dst")
	}

}
