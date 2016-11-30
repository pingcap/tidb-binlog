package drainer

import (
	"runtime"

	"github.com/ngaut/log"
)

var (
	// Version defines the version of drainer server
	Version = "1.0.0+git"

	// GitSHA will be set during make
	GitSHA = "Not provided (use make build instead of go build)"
	// BuildTS and BuildTS will be set during make
	BuildTS = "Not provided (use make build instead of go build)"
)

// PrintVersionInfo show version info to Stdout
func PrintVersionInfo() {
	log.Infof("drainer Version: %s\n", Version)
	log.Infof("Git SHA: %s\n", GitSHA)
	log.Infof("Build TS: %s\n", BuildTS)
	log.Infof("Go Version: %s\n", runtime.Version())
	log.Infof("Go OS/Arch: %s%s\n", runtime.GOOS, runtime.GOARCH)
}
