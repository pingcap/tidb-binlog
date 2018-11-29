package executor

import (
	// mysql driver
	_ "github.com/go-sql-driver/mysql"
)

// DBConfig is the DB configuration.
type DBConfig struct {
	Host          string           `toml:"host" json:"host"`
	User          string           `toml:"user" json:"user"`
	Password      string           `toml:"password" json:"password"`
	Port          int              `toml:"port" json:"port"`
	Checkpoint    CheckpointConfig `toml:"checkpoint" json:"checkpoint"`
	BinlogFileDir string           `toml:"dir" json:"dir"`
	Compression   string           `toml:"compression" json:"compression"`
	CompressLevel int              `toml:"compress-level" json:"compress-level"`
	TimeLimit     string           `toml:"time-limit" json:"time-limit"`
	SizeLimit     string           `toml:"size-limit" json:"size-limit"`

	ZKAddrs      string `toml:"zookeeper-addrs" json:"zookeeper-addrs"`
	KafkaAddrs   string `toml:"kafka-addrs" json:"kafka-addrs"`
	KafkaVersion string `toml:"kafka-version" json:"kafka-version"`
	TopicName    string `toml:"topic-name" json:"topic-name"`
	// get it from pd
	ClusterID uint64 `toml:"-" json:"-"`
}

// CheckpointConfig is the Checkpoint configuration.
type CheckpointConfig struct {
	Schema string `toml:"schema" json:"schema"`
}
