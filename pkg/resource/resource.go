package resource

import (
	"sync"
	"time"
)

type ResourceControl struct {
	mu sync.RWMutex
	// used for token
	mut sync.RWMutex

	MaxResource  uint64
	ResourceUsed uint64

	ResourceUsedMap map[string]uint64
	NumMap          map[string]uint64
	MaxResourceMap  map[string]uint64

	// if resource reach the max resource, use the resource token temporary
	GenTokenRate     uint64
	ResourceToken    uint64
	MaxResourceToken uint64

	ResourceTokenMap map[string]uint64
}

func NewResourceControl(maxResource, maxResourceToken, tokenRate uint64) *ResourceControl {
	m := &ResourceControl{
		ResourceUsedMap: make(map[string]uint64),
		NumMap:          make(map[string]uint64),
		MaxResourceMap:  make(map[string]uint64),
		MaxResource:     maxResource,

		GenTokenRate:     tokenRate,
		MaxResourceToken: maxResourceToken,
	}

	go m.background()

	return m
}

func (m *ResourceControl) Allocate(size uint64, label string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if label != "" {
		m.addNewLabel(label)
		m.ResourceUsedMap[label] += size
		m.NumMap[label] += 1
		if m.ResourceUsedMap[label] > m.MaxResourceMap[label] {
			m.applyTokenSync(label, size)
		}
		m.ResourceUsed += size
	} else {
		m.ResourceUsed += size
		if m.ResourceUsed > m.MaxResource {
			m.applyTokenSync(label, size)
		}
	}
}

func (m *ResourceControl) Free(size uint64, label string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if label != "" {
		m.ResourceUsedMap[label] -= size
		m.NumMap[label] -= 1
	}

	m.ResourceUsed -= size
}

func (m *ResourceControl) OfflineLabel(label string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.ResourceUsedMap, label)
	delete(m.MaxResourceMap, label)
	delete(m.NumMap, label)
	delete(m.ResourceTokenMap, label)
	m.MaxResourceMap = BalanceResource(m.MaxResource, m.ResourceUsed, m.ResourceUsedMap, true)
}

func (m *ResourceControl) addNewLabel(label string) {
	_, ok := m.ResourceUsedMap[label]
	if ok {
		return
	}

	m.ResourceUsedMap[label] = 0
	m.NumMap[label] = 0
	m.ResourceTokenMap[label] = 0
	m.MaxResourceMap[label] = 0
	m.MaxResourceMap = BalanceResource(m.MaxResource, m.ResourceUsed, m.ResourceUsedMap, true)
}

func (m *ResourceControl) background() {
	// time1 is used for award resource token
	timer1 := time.NewTicker(time.Second)
	// time2 is used for balance resource between label by average
	timer2 := time.NewTicker(time.Hour)
	// time2 is used for balance resource between label
	timer3 := time.NewTicker(9 * time.Minute)
	defer timer1.Stop()
	defer timer2.Stop()

	for {
		select {
		case <-timer1.C:
			m.mut.Lock()
			m.awardToken()
			m.mut.Unlock()
		case <-timer2.C:
			m.mu.Lock()
			m.MaxResourceMap = BalanceResource(m.MaxResource, m.ResourceUsed, m.ResourceUsedMap, true)
			m.mu.Unlock()
		case <-timer3.C:
			m.mu.Lock()
			m.MaxResourceMap = BalanceResource(m.MaxResource, m.ResourceUsed, m.ResourceUsedMap, false)
			m.mu.Unlock()
		default:
		}
	}
}

func (m *ResourceControl) awardToken() {
	labelNum := uint64(len(m.ResourceTokenMap))
	for label, token := range m.ResourceTokenMap {
		if token+m.GenTokenRate/labelNum > m.MaxResourceToken/labelNum {
			m.ResourceTokenMap[label] = m.MaxResourceToken / labelNum
		} else {
			m.ResourceTokenMap[label] = token + m.GenTokenRate/labelNum
		}
	}

	if m.ResourceToken+m.GenTokenRate > m.MaxResourceToken {
		m.ResourceToken = m.MaxResourceToken
	} else {
		m.ResourceToken += m.GenTokenRate
	}
}

func (m *ResourceControl) applyToken(label string, size uint64) bool {
	m.mut.Lock()
	defer m.mut.Unlock()

	if label != "" {
		if m.ResourceTokenMap[label] > size {
			m.ResourceTokenMap[label] -= size
			return true
		}
		return false
	}

	if m.ResourceToken > size {
		m.ResourceToken -= size
		return true
	}
	return false
}

func (m *ResourceControl) applyTokenSync(label string, size uint64) {
	for {
		labelSize := uint64(len(m.ResourceTokenMap))
		if labelSize == 0 {
			labelSize = 1
		}
		if size > m.MaxResourceToken/labelSize {
			time.Sleep(time.Duration(2*size/m.MaxResourceToken) * time.Second)
			m.applyToken(label, m.MaxResourceToken/labelSize)
			break
		}

		if m.applyToken(label, size) {
			break
		}

		time.Sleep(time.Second)
	}

}
