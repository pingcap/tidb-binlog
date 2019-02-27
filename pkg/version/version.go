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
	// ReleaseVersion will be set during make
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
