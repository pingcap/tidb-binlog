// Copyright 2020 PingCAP, Inc.
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

package util

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
)

var empty = ""
var _ toml.TextMarshaler = Duration(empty)
var _ toml.TextUnmarshaler = (*Duration)(&empty)
var _ json.Marshaler = Duration(empty)
var _ json.Unmarshaler = (*Duration)(&empty)

// Duration is a wrapper of time.Duration for TOML and JSON.
// it can be parsed to both integer and string
// integer 7 will be parsed to 7*24h
// string 10m will be parsed to 10m
type Duration string

// NewDuration creates a Duration from time.Duration.
func NewDuration(duration time.Duration) Duration {
	return Duration(duration.String())
}

// MarshalJSON returns the duration as a JSON string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, d)), nil
}

// UnmarshalJSON parses a JSON string into the duration.
func (d *Duration) UnmarshalJSON(text []byte) error {
	s, err := strconv.Unquote(string(text))
	if err != nil {
		return errors.WithStack(err)
	}
	td := Duration(s)
	_, err = td.ParseDuration()
	if err != nil {
		return errors.WithStack(err)
	}
	*d = Duration(s)
	return nil
}

// UnmarshalText parses a TOML string into the duration.
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	td := Duration(text)
	_, err = td.ParseDuration()
	if err != nil {
		return errors.WithStack(err)
	}
	*d = Duration(text)
	return nil
}

// MarshalText returns the duration as a JSON string.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d), nil
}

// ParseDuration parses gc durations. The default unit is day.
func (d Duration) ParseDuration() (time.Duration, error) {
	gc := string(d)
	t, err := strconv.ParseUint(gc, 10, 64)
	if err == nil {
		return time.Duration(t) * 24 * time.Hour, nil
	}
	gcDuration, err := time.ParseDuration(gc)
	if err != nil {
		return 0, errors.Annotatef(err, "unsupported gc time %s, etc: use 7 for 7 day, 7h for 7 hour", gc)
	}
	return gcDuration, nil
}
