package main

import (
	"os"
	"strconv"
	"database/sql"
	"strings"
	"flag"
	"fmt"

	_ "github.com/go-sql-driver/mysql"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/test/diff"
)

var (
	databases string
	all       bool
	url1 string
	url2 string
)

func init() {
	flag.BoolVar(&all, "A", false, "Compare all the databases. shorthand for --all-databases")
	flag.BoolVar(&all, "-all-databases", false, "Compare all the databases. This will be same as --databases with all databases selected.")

	flag.StringVar(&databases, "B", "", "Compare several databases. shorthand for --databases")
	flag.StringVar(&databases, "-databases", "", "Compare several databases. Note the difference in usage; in this case no tables are given. All name arguments are regarded as database names. 'USE db_name;' will be included in the output.")

	flag.StringVar(&url1, "url1", "root@127.0.0.1:4000", "user[:password]@host:port")
	flag.StringVar(&url2, "url2", "", "user:password@host:port")
}

func main() {
	var db1, db2 dbConf
	dbs := parseConfig(&db1, &db2)

	for _, dbName := range dbs {
		eq, err := compareOneDB(&db1, &db2, dbName)
		if err != nil {
			fmt.Println(errors.ErrorStack(err))
			return
		}
		if !eq {
			fmt.Println("false")
			return
		}
	}
	fmt.Println("true")
}

func parseConfig(db1, db2 *dbConf) []string {
	flag.Parse()
	if url1 == "" || url2 == "" {
		flag.PrintDefaults()
		os.Exit(-1)
	}

	if err := db1.fromString(url1); err != nil {
		fmt.Println("url1 error:", err)
		os.Exit(-1)
	}
	if err := db2.fromString(url2); err != nil {
		fmt.Println("url2 error:", err)
		os.Exit(-1)
	}

	if all {
		dbNames1 := showDatabases(db1)
		dbNames2 := showDatabases(db2)
		if !equalStrings(dbNames1, dbNames2) {
			fmt.Println("false")
			os.Exit(0)
		}
		return dbNames1
	}

	if databases == "" {
		flag.PrintDefaults()
		os.Exit(-1)
	}
	return strings.Split(databases , ",")
}

type dbConf struct {
	user string
	password string
	host string
	port int
}

var parseErr = errors.New("format: user[:password]@host:port")

func (dbcf *dbConf) fromString(url string) error {
	tmp := strings.Split(url, "@")
	if len(tmp) != 2 {
		return parseErr
	}

	part1 := strings.Split(tmp[0], ":")
	switch len(part1) {
	case 1:
		dbcf.user = part1[0]
	case 2:
		dbcf.user = part1[0]
		dbcf.password = part1[1]
	default:
		return parseErr
	}

	part2 := strings.Split(tmp[1], ":")
	if len(part2) != 2 {
		return parseErr
	}
	port, err := strconv.ParseInt(part2[1], 10, 64)
	if err != nil {
		return parseErr
	}

	dbcf.host = part2[0]
	dbcf.port = int(port)
	return nil
}

func (dbcf *dbConf) fullPath(dbName string) string {
	return fmt.Sprintf("%s@tcp(%s:%d)/%s?timeout=30s&strict=true",
		dbcf.user,
		dbcf.host,
		dbcf.port,
		dbName)
}

func compareOneDB(dbc1, dbc2 *dbConf, dbName string) (bool, error) {
	db1, err := sql.Open("mysql", dbc1.fullPath(dbName))
	if err != nil {
		return false, errors.Trace(err)
	}
	defer db1.Close()

	db2, err := sql.Open("mysql", dbc2.fullPath(dbName))
	if err != nil {
		return false, errors.Trace(err)
	}
	defer db2.Close()

	df := diff.New(db1, db2)
	eq, err := df.Equal()
	if err != nil {
		return false, errors.Trace(err)
	}
	return eq, nil
}

func showDatabases(dbcf *dbConf) []string {
	src := fmt.Sprintf("%s@tcp(%s:%d)?timeout=30s&strict=true",
		dbcf.user,
		dbcf.host,
		dbcf.port)
	db, err := sql.Open("mysql", src)
	if err != nil {
		fmt.Println("database config error:", err)
		os.Exit(-1)
	}
	defer db.Close()

	ret, err := diff.ShowDatabases(db)
	if err != nil {
		fmt.Println(errors.ErrorStack(err))
		os.Exit(-1)
	}
	return ret
}

func equalStrings(str1, str2 []string) bool {
	if len(str1) != len(str2) {
		return false
	}
	for i := 0; i < len(str1); i++ {
		if str1[i] != str2[i] {
			return false
		}
	}
	return true
}
