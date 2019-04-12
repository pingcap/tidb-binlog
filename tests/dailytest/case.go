package dailytest

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
)

// test different data type of mysql
// mysql will change boolean to tinybit(1)
var case1 = []string{`
CREATE TABLE  binlog_case1 (
	id INT AUTO_INCREMENT,
	t_boolean BOOLEAN,
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
	PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
`,
	`
INSERT INTO binlog_case1(t_boolean, t_bigint, t_double, t_decimal, t_bit
	,t_date, t_datetime, t_timestamp, t_time, t_year
	,t_char, t_varchar, t_blob, t_text, t_enum
	,t_set, t_json) VALUES
	(true, 9223372036854775807, 123.123, 123456789012.123456789012, b'1000001'
	,'1000-01-01', '9999-12-31 23:59:59', '19731230153000', '23:59:59', 1970
	,'测', '测试', 'blob', '测试text', 'enum2'
	,'a,b', NULL);
`,
	`
INSERT INTO binlog_case1(t_boolean) VALUES(TRUE);
`,
	`
INSERT INTO binlog_case1(t_boolean) VALUES(FALSE);
`,
	// minmum value of bigint
	`
INSERT INTO binlog_case1(t_bigint) VALUES(-9223372036854775808);
`,
	// maximum value of bigint
	`
INSERT INTO binlog_case1(t_bigint) VALUES(9223372036854775807);
`,
	`
INSERT INTO binlog_case1(t_json) VALUES('{"key1": "value1", "key2": "value2"}');
`,
}

var case1Clean = []string{`
	DROP TABLE binlog_case1`,
}

// https://internal.pingcap.net/jira/browse/TOOL-714
var case2 = []string{`
CREATE TABLE binlog_case2 (id INT, a1 INT, a3 INT, UNIQUE KEY dex1(a1, a3));
`,
	`
INSERT INTO binlog_case2(id, a1, a3) VALUES(1, 1, NULL);
`,
	`
INSERT INTO binlog_case2(id, a1, a3) VALUES(2, 1, NULL);
`,
	`
UPDATE binlog_case2 SET id = 10 WHERE id = 1;
`,
	`
UPDATE binlog_case2 SET id = 100 WHERE id = 10;
`,
}

var case2Clean = []string{`
	DROP TABLE binlog_case2`,
}

var case3 = []string{`
CREATE TABLE a(id INT PRIMARY KEY, a1 INT);
`,
	`
INSERT INTO a(id, a1) VALUES(1,1),(2,1);
`,
	`
ALTER TABLE a ADD UNIQUE INDEX aidx(a1);
`,
}

var case3Clean = []string{
	`DROP TABLE a`,
}

type testRunner struct {
	src    *sql.DB
	dst    *sql.DB
	schema string
}

func (tr *testRunner) run(test func(*sql.DB)) {
	RunTest(tr.src, tr.dst, tr.schema, test)
}

func (tr *testRunner) execSQLs(sqls []string) {
	RunTest(tr.src, tr.dst, tr.schema, func(src *sql.DB) {
		err := execSQLs(tr.src, sqls)
		if err != nil {
			log.Fatal(err)
		}
	})
}

// RunCase run some simple test case
func RunCase(src *sql.DB, dst *sql.DB, schema string) {
	tr := &testRunner{src: src, dst: dst, schema: schema}

	tr.execSQLs(case1)
	tr.execSQLs(case1Clean)

	tr.execSQLs(case2)
	tr.execSQLs(case2Clean)

	// run case3
	tr.run(func(src *sql.DB) {
		err := execSQLs(src, case3)
		if err != nil && !strings.Contains(err.Error(), "Duplicate for key") {
			log.Fatal(err)
		}
	})
	tr.execSQLs(case3Clean)

	tr.run(caseTblWithGeneratedCol)
	tr.execSQLs([]string{"DROP TABLE gen_contacts;"})

	tr.run(caseCreateView)
	tr.execSQLs([]string{"DROP VIEW view_user_sum;"})
	tr.execSQLs([]string{"DROP TABLE base_for_view;"})

	// random op on have both pk and uk table
	tr.run(func(src *sql.DB) {
		start := time.Now()

		err := updatePKUK(src, 1000)
		if err != nil {
			log.Fatal(errors.ErrorStack(err))
		}

		log.Info(" updatePKUK take: ", time.Since(start))
	})

	// swap unique index value
	tr.run(func(src *sql.DB) {
		_, err := src.Exec("create table uindex(id int primary key, a1 int unique)")
		if err != nil {
			log.Fatal(err)
		}

		_, err = src.Exec("insert into uindex(id, a1) values(1, 10),(2,20)")
		if err != nil {
			log.Fatal(err)
		}

		tx, err := src.Begin()
		if err != nil {
			log.Fatal(err)
		}

		_, err = tx.Exec("update uindex set a1 = 30 where id = 1")
		if err != nil {
			log.Fatal(err)
		}

		_, err = tx.Exec("update uindex set a1 = 10 where id = 2")
		if err != nil {
			log.Fatal(err)
		}

		_, err = tx.Exec("update uindex set a1 = 20 where id = 1")
		if err != nil {
			log.Fatal(err)
		}

		err = tx.Commit()
		if err != nil {
			log.Fatal(err)
		}

		_, err = src.Exec("drop table uindex")
		if err != nil {
			log.Fatal(err)
		}
	})

	// test big binlog msg
	tr.run(func(src *sql.DB) {
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
			_, err = tx.Query("INSERT INTO binlog_big(id, data) VALUES(?, ?);", i, data)
			if err != nil {
				log.Fatal(err)
			}
		}
		err = tx.Commit()
		if err != nil {
			log.Fatal(err)
		}
	})
	tr.execSQLs([]string{"DROP TABLE binlog_big;"})
}

// caseTblWithGeneratedCol creates a table with generated column,
// and insert values into the table
func caseTblWithGeneratedCol(db *sql.DB) {
	_, err := db.Exec(`
CREATE TABLE gen_contacts (
	id INT AUTO_INCREMENT PRIMARY KEY,
	first_name VARCHAR(50) NOT NULL,
	last_name VARCHAR(50) NOT NULL,
	other VARCHAR(101),
	fullname VARCHAR(101) GENERATED ALWAYS AS (CONCAT(first_name,' ',last_name)),
	initial VARCHAR(101) GENERATED ALWAYS AS (CONCAT(LEFT(first_name, 1),' ',LEFT(last_name,1))) STORED
);`)
	if err != nil {
		log.Fatal(err)
	}

	insertSQL := "INSERT INTO gen_contacts(first_name, last_name) VALUES(?, ?);"
	updateSQL := "UPDATE gen_contacts SET other = fullname WHERE first_name = ?"
	for i := 0; i < 64; i++ {
		_, err := db.Query(insertSQL, fmt.Sprintf("John%d", i), fmt.Sprintf("Dow%d", i))
		if err != nil {
			log.Fatal(err)
		}

		idxToUpdate := rand.Intn(i + 1)
		_, err = db.Query(updateSQL, fmt.Sprintf("John%d", idxToUpdate))
		if err != nil {
			log.Fatal(err)
		}
	}
	delSQL := "DELETE FROM gen_contacts WHERE fullname = ?"
	for i := 0; i < 10; i++ {
		if _, err = db.Query(delSQL, fmt.Sprintf("John%d Dow%d", i)); err != nil {
			log.Fatal(err)
		}
	}
}

func caseCreateView(db *sql.DB) {
	_, err := db.Exec(`
CREATE TABLE base_for_view (
	id INT AUTO_INCREMENT PRIMARY KEY,
	user_id INT NOT NULL,
	amount INT NOT NULL
);`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`
CREATE VIEW view_user_sum (user_id, total)
AS SELECT user_id, SUM(amount) FROM base_for_view GROUP BY user_id;`)
	if err != nil {
		log.Fatal(err)
	}

	insertSQL := "INSERT INTO base_for_view(user_id, amount) VALUES(?, ?);"
	for i := 0; i < 42; i++ {
		userID := i * 10
		for j := 0; j < 3; j++ {
			_, err := db.Exec(insertSQL, userID, j*10+i)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

// updatePKUK create a table with primary key and unique key
// then do opNum randomly DML
func updatePKUK(db *sql.DB, opNum int) error {
	maxKey := 20
	_, err := db.Exec("create table pkuk(pk int primary key, uk int, v int, unique key uk(uk));")
	if err != nil {
		return errors.Trace(err)
	}

	var pks []int
	addPK := func(pk int) {
		pks = append(pks, pk)
	}
	removePK := func(pk int) {
		var tmp []int
		for _, v := range pks {
			if v != pk {
				tmp = append(tmp, v)
			}
		}
		pks = tmp
	}
	hasPK := func(pk int) bool {
		for _, v := range pks {
			if v == pk {
				return true
			}
		}
		return false
	}

	for i := 0; i < opNum; {
		var sql string
		pk := rand.Intn(maxKey)
		uk := rand.Intn(maxKey)
		v := rand.Intn(10000)
		oldPK := rand.Intn(maxKey)

		// try randomly insert&update&delete
		op := rand.Intn(3)
		switch op {
		case 0:
			if len(pks) == maxKey {
				continue
			}
			for hasPK(pk) {
				log.Info(pks)
				pk = rand.Intn(maxKey)
			}
			sql = fmt.Sprintf("insert into pkuk(pk, uk, v) values(%d,%d,%d)", pk, uk, v)
		case 1:
			if len(pks) == 0 {
				continue
			}
			for !hasPK(oldPK) {
				log.Info(pks)
				oldPK = rand.Intn(maxKey)
			}
			sql = fmt.Sprintf("update pkuk set pk = %d, uk = %d, v = %d where pk = %d", pk, uk, v, oldPK)
		case 2:
			if len(pks) == 0 {
				continue
			}
			for !hasPK(pk) {
				log.Info(pks)
				pk = rand.Intn(maxKey)
			}
			sql = fmt.Sprintf("delete from pkuk where pk = %d", pk)
		}

		_, err := db.Exec(sql)
		if err != nil {
			// for insert and update, we didn't check for uk's duplicate
			if strings.Contains(err.Error(), "Duplicate entry") {
				continue
			}
			return errors.Trace(err)
		}

		switch op {
		case 0:
			addPK(pk)
		case 1:
			removePK(oldPK)
			addPK(pk)
		case 2:
			removePK(pk)
		}
		i++
	}

	_, err = db.Exec("DROP TABLE pkuk")
	return errors.Trace(err)
}
