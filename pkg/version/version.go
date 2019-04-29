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
	"fmt"
	"runtime"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	// GitHash will be set during make
	GitHash = "Not provided (use make build instead of go build)"
	// BuildTS and BuildTS will be set during make
	BuildTS = "Not provided (use make build instead of go build)"
	// ReleaseVersion will be set during make, default value is v1.0
	ReleaseVersion = "Not provided (use make build instead of go build)"
)

// GetRawVersionInfo do what its name tells
func GetRawVersionInfo() string {
	var info string
	info += fmt.Sprintf("Release Version: %s\n", ReleaseVersion)
	info += fmt.Sprintf("Git Commit Hash: %s\n", GitHash)
	info += fmt.Sprintf("Build TS: %s\n", BuildTS)
	info += fmt.Sprintf("Go Version: %s\n", runtime.Version())
	info += fmt.Sprintf("Go OS/Arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
	return info
}

// PrintVersionInfo print version info.
func PrintVersionInfo(app string) {
	oldLevel := log.GetLevel()
	log.SetLevel(zap.InfoLevel)
	defer log.SetLevel(oldLevel)

	log.Info("Welcome to "+app,
		zap.String("Release Version", ReleaseVersion),
		zap.String("Git Commit Hash", GitHash),
		zap.String("Build TS", BuildTS),
		zap.String("Go Version", runtime.Version()),
		zap.String("Go OS/Arch", runtime.GOOS+"/"+runtime.GOARCH),
	)
}
