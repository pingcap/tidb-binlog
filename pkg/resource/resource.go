package resource

import (
	"sync"
	"time"
)

const (
	awardTokenDuration             = time.Second
	balanceResourceAverageDuration = time.Hour
	balanceResourceByUsedDuration  = 9 * time.Minute
)

// Resource is a struct for Resource
type Resource struct {
	Max   uint64
	Num   uint64
	Used  uint64
	Token uint64
}

// NewResource returns a new Resource
func NewResource(max uint64) *Resource {
	return &Resource{
		Max: max,
	}
}

// ReachMax returns true if used is gt max
func (r *Resource) ReachMax() bool {
	return r.Used > r.Max
}

// Control controls the resource
type Control struct {
	mu sync.RWMutex
	// used for token
	mut sync.RWMutex

	MaxResource  uint64
	ResourceUsed uint64

	ResourceBucket map[string]*Resource

	// if resource reach the max resource, use the resource token temporary
	GenTokenRate     uint64
	ResourceToken    uint64
	MaxResourceToken uint64

	ResourceBalancer Balancer
}

// NewControl creates a new Control
func NewControl(maxResource uint64) *Control {
	maxResourceToken := maxResource / 10
	tokenRate := maxResource / 1000
	resourceBucket := make(map[string]*Resource)
	m := &Control{
		MaxResource:      maxResource,
		GenTokenRate:     tokenRate,
		MaxResourceToken: maxResourceToken,
		ResourceBucket:   resourceBucket,
		ResourceBalancer: NewResBalance(resourceBucket),
	}

	go m.background()

	return m
}

// Allocate allocates resource
func (m *Control) Allocate(size uint64, owner string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if owner == "" {
		owner = "all"
	}

	m.addNewOwner(owner)
	m.ResourceBucket[owner].Used += size
	m.ResourceBucket[owner].Num++
	if m.ResourceBucket[owner].ReachMax() {
		m.applyTokenSync(owner, size)
	}
	m.ResourceUsed += size
}

// Free frees the resource
func (m *Control) Free(size uint64, owner string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if owner == "" {
		owner = "all"
	}

	m.ResourceBucket[owner].Used -= size
	m.ResourceBucket[owner].Num--
	m.ResourceUsed -= size
}

// OfflineOwner offlines the owner
func (m *Control) OfflineOwner(owner string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.ResourceBucket, owner)
	m.ResourceBalancer.Apply(m.MaxResource, m.ResourceUsed, average)
}

func (m *Control) addNewOwner(owner string) {
	_, ok := m.ResourceBucket[owner]
	if ok {
		return
	}

	m.ResourceBucket[owner] = NewResource(0)
	m.ResourceBalancer.Apply(m.MaxResource, m.ResourceUsed, average)
}

func (m *Control) background() {
	// time1 is used for award resource token
	timer1 := time.NewTicker(awardTokenDuration)
	// time2 is used for balance resource between owner by average
	timer2 := time.NewTicker(balanceResourceAverageDuration)
	// time2 is used for balance resource between owner by used
	timer3 := time.NewTicker(balanceResourceByUsedDuration)
	defer timer1.Stop()
	defer timer2.Stop()
	defer timer3.Stop()

	for {
		select {
		case <-timer1.C:
			m.mut.Lock()
			m.awardToken()
			m.mut.Unlock()
		case <-timer2.C:
			m.mu.Lock()
			m.ResourceBalancer.Apply(m.MaxResource, m.ResourceUsed, average)
			m.mu.Unlock()
		case <-timer3.C:
			m.mu.Lock()
			m.ResourceBalancer.Apply(m.MaxResource, m.ResourceUsed, proportion)
			m.mu.Unlock()
		default:
		}
	}
}

func (m *Control) awardToken() {
	m.mut.Lock()
	defer m.mut.Unlock()

	ownerNum := uint64(len(m.ResourceBucket))
	for _, resource := range m.ResourceBucket {
		if resource.Token+m.GenTokenRate/ownerNum > m.MaxResourceToken/ownerNum {
			resource.Token = m.MaxResourceToken / ownerNum
		} else {
			resource.Token += m.GenTokenRate / ownerNum
		}
	}

	if m.ResourceToken+m.GenTokenRate > m.MaxResourceToken {
		m.ResourceToken = m.MaxResourceToken
	} else {
		m.ResourceToken += m.GenTokenRate
	}
}

func (m *Control) applyToken(owner string, size uint64) bool {
	m.mut.Lock()
	defer m.mut.Unlock()

	if owner == "" {
		owner = "all"
	}

	if m.ResourceBucket[owner].Token > size {
		m.ResourceBucket[owner].Token -= size
		return true
	}
	return false
}

func (m *Control) applyTokenSync(owner string, size uint64) {
	for {
		ownerSize := uint64(len(m.ResourceBucket))
		if ownerSize == 0 {
			return
		}
		if size > m.MaxResourceToken/ownerSize {
			time.Sleep(time.Duration(2*size/m.MaxResourceToken) * time.Second)
			m.applyToken(owner, m.MaxResourceToken/ownerSize)
			break
		}

		if m.applyToken(owner, size) {
			break
		}

		time.Sleep(time.Second)
	}
}
