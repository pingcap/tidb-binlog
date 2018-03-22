package flow

import (
	"sync"
	"time"

	"github.com/ngaut/log"
	//"golang.org/x/net/context"

	//"github.com/juju/errors"
)

type SpeedControl struct {
	Rate     uint64
	Token    uint64
	MaxToken uint64
	Interval uint64 // second
	Mu       sync.Mutex
}

func NewSpeedControl(rate, maxToken, interval uint64) *SpeedControl {
	s := &SpeedControl{
		Rate:     rate,
		MaxToken: maxToken,
		Interval: interval,
	}
	go s.AwardToken()

	return s
}

func (f *SpeedControl)AwardToken() {
	timer := time.NewTicker(time.Duration(f.Interval)*time.Second)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			f.Mu.Lock()
			if f.Token + f.Rate*f.Interval > f.MaxToken {
				f.Token = f.MaxToken
			} else {
				f.Token += f.Rate*f.Interval
			}
			log.Debugf("award token %d", f.Token)
			f.Mu.Unlock()
		default:
		}
	}
}

func (f *SpeedControl)ApplyToken() bool {
	f.Mu.Lock()
	defer f.Mu.Unlock()

	if f.Token >= 1 {
		f.Token -= 1
		return true
	}
	
	return false
}

func (f *SpeedControl)ApplyTokenSync() {
	for {
		apply := f.ApplyToken()
		if apply {
			break
		}

		time.Sleep(time.Second)
	}
}