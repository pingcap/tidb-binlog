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

// +build linux

package storage

import "syscall"

// fdatasync() is similar to fsync(), but does not flush modified metadata unless that metadata is needed in order to allow a subsequent data retrieval to be correctly handled
// https://linux.die.net/man/2/fdatasync
// in some os don't support fdatasync, we can just use sync
func (lf *logFile) fdatasync() error {
	return syscall.Fdatasync(int(lf.fd.Fd()))
}
