package resource

const (
	average    = "average"
	proportion = "proportion"
)

// Balancer is used for balance something
type Balancer interface {
	Apply(total, used uint64, algorithm string)
}

// ResBalance is used for balance resource
type ResBalance struct {
	Resources map[string]*Resource
}

// NewResBalance retuns a new ResBalance
func NewResBalance(resources map[string]*Resource) Balancer {
	return &ResBalance{
		Resources: resources,
	}
}

// Apply implement Balancer.Apply interface
func (b *ResBalance) Apply(total, used uint64, algorithm string) {
	if len(b.Resources) == 0 {
		return
	}

	// use basicResource avoid some label's max resource is too small
	num := uint64(len(b.Resources))
	basicResource := total / num / 2

	for _, resource := range b.Resources {
		switch algorithm {
		case average:
			resource.Max = total / num
		case proportion:
			resource.Max = basicResource + total/2*(resource.Used/used)
		default:
			return
		}
	}
}
