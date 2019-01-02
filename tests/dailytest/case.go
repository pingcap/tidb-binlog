package dailytest

import (
	"database/sql"
	"strings"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/diff"
)

// test different data type of mysql
// mysql will change boolean to tinybit(1)
var case1 = []string{`
create table  binlog_case1 (
	id int auto_increment,
	t_boolean boolean,
	t_bigint BIGINT,
	t_double DOUBLE,
	t_decimal DECIMAL(38,19),
	t_bit BIT(64),
	t_date DATE,
	t_datetime DATETIME,
	t_timestamp TIMESTAMP NULL,
	t_time TIME,
	t_year YEAR,
	t_char CHAR,
	t_varchar VARCHAR(10),
	t_blob BLOB,
	t_text TEXT,
	t_enum ENUM('enum1', 'enum2', 'enum3'),
	t_set SET('a', 'b', 'c'),
	t_json JSON,
	primary key(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
`,
	`
insert into binlog_case1(t_boolean, t_bigint, t_double, t_decimal, t_bit
	,t_date, t_datetime, t_timestamp, t_time, t_year
	,t_char, t_varchar, t_blob, t_text, t_enum
	,t_set, t_json) values
	(true, 9223372036854775807, 123.123, 123456789012.123456789012, b'1000001'
	,'1000-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 1970
	,'测', '测试', 'blob', '测试text', 'enum2'
	,'a,b', NULL);
`,
	`
insert into binlog_case1(t_boolean) values(TRUE);
`,
	`
insert into binlog_case1(t_boolean) values(FALSE);
`,
	// minmum value of bigint
	`
insert into binlog_case1(t_bigint) values(-9223372036854775808);
`,
	// maximum value of bigint
	`
insert into binlog_case1(t_bigint) values(9223372036854775807);
`,
	`
insert into binlog_case1(t_json) values('{"key1": "value1", "key2": "value2"}');
`,
}

var case1Clean = []string{`
	drop table binlog_case1`,
}

// https://internal.pingcap.net/jira/browse/TOOL-714
var case2 = []string{`
create table binlog_case2 (id int, a1 int, a3 int, unique key dex1(a1, a3));
`,
	`
insert into binlog_case2(id, a1, a3) values(1, 1, NULL);
`,
	`
insert into binlog_case2(id, a1, a3) values(2, 1, NULL);
`,
	`
update binlog_case2 set id = 10 where id = 1;
`,
	`
update binlog_case2 set id = 100 where id = 10;
`,
}

var case2Clean = []string{`
	drop table binlog_case2`,
}

var case3 = []string{`
create table a(id int primary key, a1 int);
`,
	`
insert into a(id, a1) values(1,1),(2,1);
`,
	`
alter table a add unique index aidx(a1);
`,
}

var case3Clean = []string{`
	drop table a`,
}

// RunCase run some simple test case
func RunCase(cfg *diff.Config, src *sql.DB, dst *sql.DB) {
	RunTest(cfg, src, dst, func(src *sql.DB) {
		err := execSQLs(src, case1)
		if err != nil {
			log.Fatal(err)
		}
	})

	// clean table
	RunTest(cfg, src, dst, func(src *sql.DB) {
		err := execSQLs(src, case1Clean)
		if err != nil {
			log.Fatal(err)
		}
	})

	// run case2
	RunTest(cfg, src, dst, func(src *sql.DB) {
		err := execSQLs(src, case2)
		if err != nil {
			log.Fatal(err)
		}
	})

	// clean table
	RunTest(cfg, src, dst, func(src *sql.DB) {
		err := execSQLs(src, case2Clean)
		if err != nil {
			log.Fatal(err)
		}
	})

	// run case3
	RunTest(cfg, src, dst, func(src *sql.DB) {
		err := execSQLs(src, case3)
		if err != nil && !strings.Contains(err.Error(), "Duplicate for key") {
			log.Fatal(err)
		}
	})

	// clean table
	RunTest(cfg, src, dst, func(src *sql.DB) {
		err := execSQLs(src, case3Clean)
		if err != nil {
			log.Fatal(err)
		}
	})

	// test big binlog msg
	RunTest(cfg, src, dst, func(src *sql.DB) {
		_, err := src.Query("create table binlog_big(id int primary key, data longtext);")
		if err != nil {
			log.Fatal(err)
		}

		tx, err := src.Begin()
		if err != nil {
			log.Fatal(err)
		}
		// insert 50 * 1M
		// note limitation of TiDB: https://github.com/pingcap/docs/blob/733a5b0284e70c5b4d22b93a818210a3f6fbb5a0/FAQ.md#the-error-message-transaction-too-large-is-displayed
		var data = make([]byte, 1<<20)
		for i := 0; i < 50; i++ {
			_, err = tx.Query("insert into binlog_big(id, data) values(?, ?);", i, data)
			if err != nil {
				log.Fatal(err)
			}
		}
		err = tx.Commit()
		if err != nil {
			log.Fatal(err)
		}
	})
	// clean table
	RunTest(cfg, src, dst, func(src *sql.DB) {
		_, err := src.Query("drop table binlog_big;")
		if err != nil {
			log.Fatal(err)
		}
	})

}
