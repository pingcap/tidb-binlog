package arbiter

import (
	"database/sql"
	"fmt"

	"github.com/juju/errors"
)

const (
	// StatusNormal is server quit normally, data <= ts is synced to downstream
	StatusNormal int = 0
	// StatusRunning is server running or quit abnormally, part of data may or may not been synced to downstream
	StatusRunning int = 1
)

// Checkpoint to save the checkpoint
type Checkpoint struct {
	database  string
	table     string
	db        *sql.DB
	topicName string
}

// NewCheckpoint creates a Checkpoint
func NewCheckpoint(db *sql.DB, topicName string) (cp *Checkpoint, err error) {
	cp = &Checkpoint{
		db:        db,
		database:  "tidb_binlog",
		table:     "arbiter_checkpoint",
		topicName: topicName,
	}

	err = cp.createSchemaIfNeed()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return cp, nil
}

func (c *Checkpoint) createSchemaIfNeed() error {
	sql := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", c.database)
	_, err := c.db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}

	sql = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s(
		topic_name VARCHAR(255) PRIMARY KEY, ts BIGINT, status INT)`,
		c.database, c.table)
	_, err = c.db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Save saves the ts and status
func (c *Checkpoint) Save(ts int64, status int) error {
	sql := fmt.Sprintf(`INSERT INTO %s.%s(topic_name, ts, status) VALUES(?,?,?)
						ON DUPLICATE KEY UPDATE
						ts = VALUES(ts), status = VALUES(status)`,
		c.database, c.table)
	_, err := c.db.Exec(sql, c.topicName, ts, status)
	if err != nil {
		return errors.Annotatef(err, "exec fail: '%s', args: %s %d, %d", sql, c.topicName, ts, status)
	}

	return nil
}

// Load return ts and status, if no record in checkpoint, return err = errors.NotFoundf
func (c *Checkpoint) Load() (ts int64, status int, err error) {
	sql := fmt.Sprintf("SELECT ts, status FROM %s.%s WHERE topic_name = '%s'",
		c.database, c.table, c.topicName)

	rows, err := c.db.Query(sql)
	if err != nil {
		return 0, 0, errors.Annotatef(err, "exec fail: '%s'", sql)
	}

	if rows.Next() {
		err := rows.Scan(&ts, &status)
		if err != nil {
			return 0, 0, errors.Trace(err)
		}
	} else {
		return 0, 0, errors.NotFoundf("no checkpoint for: %s", c.topicName)
	}

	return
}
