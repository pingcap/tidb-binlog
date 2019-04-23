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

package version

import (
	"runtime"

	"github.com/ngaut/log"
)

var (
	// GitHash will be set during make
	GitHash = "Not provided (use make build instead of go build)"
	// BuildTS and BuildTS will be set during make
	BuildTS = "Not provided (use make build instead of go build)"
	// ReleaseVersion will be set during make, default value is v1.0
	ReleaseVersion = "Not provided (use make build instead of go build)"
)

// PrintVersionInfo show version info to Stdout
func PrintVersionInfo() {
	log.Infof("Release Version: %s", ReleaseVersion)
	log.Infof("Git Commit Hash: %s", GitHash)
	log.Infof("Build TS: %s", BuildTS)
	log.Infof("Go Version: %s", runtime.Version())
	log.Infof("Go OS/Arch: %s%s", runtime.GOOS, runtime.GOARCH)
}
