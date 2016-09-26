package server

import "github.com/ngaut/log"

// InitLogger initalizes Pump's logger.
func InitLogger(isDebug bool) {
	if isDebug {
		log.SetLevelByString("debug")
	} else {
		log.SetLevelByString("info")
	}
	log.SetHighlighting(false)
}
