package pump

import (
	"github.com/ngaut/log"
	"runtime"
)

var (
	Version = "1.0.0+git"

	// GitSHA and BuildTS will be set during make
	GitSHA  = "Not provided (use make build instead of go build)"
	BuildTS = "Not provided (use make build instead of go build)"
)

func PrintVersionInfo() {
	log.Infof("pump Version: %s\n", Version)
	log.Infof("Git SHA: %s\n", GitSHA)
	log.Infof("Build TS: %s\n", BuildTS)
	log.Infof("Go Version: %s\n", runtime.Version())
	log.Infof("Go OS/Arch: %s%s\n", runtime.GOOS, runtime.GOARCH)
}
