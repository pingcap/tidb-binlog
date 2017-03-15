package pump

import (
	"runtime"

	"github.com/ngaut/log"
)

var (
	// Version defines the version of pump
	Version = "2.0.0+git"

	// GitHash will be set during make
	GitHash = "Not provided (use make build instead of go build)"
	// BuildTS and BuildTS will be set during make
	BuildTS = "Not provided (use make build instead of go build)"
)

// PrintVersionInfo show version info to Stdout
func PrintVersionInfo() {
	log.Infof("pump Version: %s", Version)
	log.Infof("Git Commit Hash: %s", GitHash)
	log.Infof("Build TS: %s", BuildTS)
	log.Infof("Go Version: %s", runtime.Version())
	log.Infof("Go OS/Arch: %s%s", runtime.GOOS, runtime.GOARCH)
}
