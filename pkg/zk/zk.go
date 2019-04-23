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

package zk

import (
	"encoding/json"
	"fmt"
	"net"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/samuel/go-zookeeper/zk"
)

// ParseConnectionString parses a zookeeper connection string in the form of
// host1:2181,host2:2181/chroot and returns the list of servers, and the chroot.
func ParseConnectionString(connectionString string) (nodes []string, chroot string) {
	nodesAndChroot := strings.SplitN(connectionString, "/", 2)
	if len(nodesAndChroot) == 2 {
		chroot = fmt.Sprintf("/%s", nodesAndChroot[1])
	}
	nodes = strings.Split(nodesAndChroot[0], ",")
	return
}

// Config for zookeeper client.
type Config struct {
	Chroot         string
	SessionTimeout time.Duration
	DialTimeout    time.Duration
}

// NewDefaultConfig creates a default config.
func NewDefaultConfig() *Config {
	return &Config{Chroot: "", SessionTimeout: time.Second * 60, DialTimeout: time.Second * 1}
}

// Client is a simple wrapper for zk.
type Client struct {
	conn Conn
	conf *Config
}

// Conn is an interface abstracting away the zookeeper connection.
//
// The standard `*zk.Conn` type implements this interface.
type Conn interface {
	Close()
	Children(path string) ([]string, *zk.Stat, error)
	Get(path string) ([]byte, *zk.Stat, error)
}

// New creates a instance of Client.
func New(servers []string, conf *Config) (*Client, error) {
	if conf == nil {
		conf = NewDefaultConfig()
	}

	dialer := func(network, address string, timeout time.Duration) (net.Conn, error) {
		return (&net.Dialer{
			Timeout:   conf.DialTimeout, // ignore timeout , since we want to set our own DialTimeout.
			KeepAlive: time.Second * 60,
		}).Dial(network, address)
	}
	conn, _, err := zk.Connect(servers, conf.SessionTimeout, zk.WithDialer(dialer))
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Client{conn: conn, conf: conf}, nil
}

// NewFromConnectionString creates a new connection instance based on a zookeeer connection string that can include a chroot.
func NewFromConnectionString(connectionString string, dialTimeout, sessionTimeout time.Duration) (*Client, error) {
	nodes, chroot := ParseConnectionString(connectionString)
	conf := NewDefaultConfig()
	conf.Chroot = chroot
	if dialTimeout != 0 {
		conf.DialTimeout = dialTimeout
	}
	if sessionTimeout != 0 {
		conf.SessionTimeout = sessionTimeout
	}
	return New(nodes, conf)
}

// NewWithConnection creates a Client with an existing zookeeper connection.
func NewWithConnection(conn Conn, conf *Config) *Client {
	if conf == nil {
		conf = NewDefaultConfig()
	}
	return &Client{conn, conf}
}

// Close closes zookeeper client.
func (c *Client) Close() {
	c.conn.Close()
}

// Topics returns a list of all registered Kafka topics.
func (c *Client) Topics() ([]string, error) {
	root := fmt.Sprintf("%s/brokers/topics", c.conf.Chroot)
	topics, _, err := c.conn.Children(root)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return topics, nil
}

// Partitions returns partitions of specific topic.
func (c *Client) Partitions(topic string) ([]int32, error) {
	metadataPath := fmt.Sprintf("%s/brokers/topics/%s", c.conf.Chroot, topic)
	value, _, err := c.conn.Get(metadataPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var tm topicMetadata
	if err := json.Unmarshal(value, &tm); err != nil {
		return nil, errors.Trace(err)
	}

	partitions := make([]int32, 0, len(tm.Partitions))
	for p := range tm.Partitions {
		partitionID, err := strconv.ParseInt(p, 10, 32)
		if err != nil {
			return nil, errors.Trace(errors.Annotatef(err, "parse partition id to int"))
		}

		partitions = append(partitions, int32(partitionID))
	}

	return partitions, nil
}

type topicMetadata struct {
	Version    int                `json:"version"`
	Partitions map[string][]int32 `json:"partitions"`
}

type brokerEntry struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

// Brokers returns a map of all the brokers that make part of the
// Kafka cluster that is registered in Zookeeper.
func (c *Client) Brokers() (map[int32]string, error) {
	root := fmt.Sprintf("%s/brokers/ids", c.conf.Chroot)
	children, _, err := c.conn.Children(root)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := make(map[int32]string)
	for _, child := range children {
		brokerID, err := strconv.ParseInt(child, 10, 32)
		if err != nil {
			return nil, errors.Trace(err)
		}

		value, _, err := c.conn.Get(path.Join(root, child))
		if err != nil {
			return nil, errors.Trace(err)
		}

		var brokerNode brokerEntry
		if err := json.Unmarshal(value, &brokerNode); err != nil {
			return nil, errors.Trace(err)
		}

		result[int32(brokerID)] = fmt.Sprintf("%s:%d", brokerNode.Host, brokerNode.Port)
	}

	return result, nil
}

// KafkaUrls get the kafka urls from zookeeper
func (c *Client) KafkaUrls() (string, error) {
	brokerMap, err := c.Brokers()
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(brokerMap) == 0 {
		return "", errors.New("kafka brokers not found in zookeeper")
	}

	kafkaUrlsArray := make([]string, 0, len(brokerMap))
	for _, url := range brokerMap {
		kafkaUrlsArray = append(kafkaUrlsArray, url)
	}

	kafkaUrls := strings.Join(kafkaUrlsArray, ",")
	return kafkaUrls, nil
}
