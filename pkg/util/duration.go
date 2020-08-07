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
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/errors"
)

// Duration is a wrapper of time.Duration for TOML and JSON.
type Duration struct {
	time.Duration
}

// NewDuration creates a Duration from time.Duration.
func NewDuration(duration time.Duration) Duration {
	return Duration{Duration: duration}
}

// MarshalJSON returns the duration as a JSON string.
func (d *Duration) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, d.String())), nil
}

// UnmarshalJSON parses a JSON string into the duration.
func (d *Duration) UnmarshalJSON(text []byte) error {
	s, err := strconv.Unquote(string(text))
	if err != nil {
		return errors.WithStack(err)
	}
	duration, err := time.ParseDuration(s)
	if err != nil {
		return errors.WithStack(err)
	}
	d.Duration = duration
	return nil
}

// UnmarshalText parses a TOML string into the duration.
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	duration, err := ParseDuration(string(text))
	if err != nil {
		return errors.WithStack(err)
	}
	d.Duration = duration.Duration
	return nil
}

// MarshalText returns the duration as a JSON string.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// ParseDuration parses gc durations. The default unit is day.
func ParseDuration(gc string) (Duration, error) {
	d, err := strconv.ParseUint(gc, 10, 64)
	if err == nil {
		return Duration{time.Duration(d) * 24 * time.Hour}, nil
	}
	gcDuration, err := time.ParseDuration(gc)
	if err != nil {
		return Duration{0}, errors.Annotatef(err, "unsupported gc time %s, etc: use 7 for 7 day, 7h for 7 hour", gc)
	}
	return Duration{gcDuration}, nil
}