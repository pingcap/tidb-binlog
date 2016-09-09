package pump

import (
	"github.com/ngaut/log"
)

// InitLogger initalizes Pump's logger.
func InitLogger(cfg *Config) {
	if cfg.Debug {
		log.SetLevelByString("debug")
	} else {
		log.SetLevelByString("info")
	}
	log.SetHighlighting(false)
}
