package resource

// BalanceResource balances the resource
func BalanceResource(total, used uint64, resources map[string]*Resource, average bool) {
	if resources == nil || len(resources) == 0 {
		return
	}

	// use basicResource avoid some label's max resource is too small
	num := uint64(len(resources))
	basicResource := total / num / 2

	for _, resource := range resources {
		if average {
			resource.Max = total / num
		} else {
			resource.Max = basicResource + total/2*(resource.Used/used)
		}
	}
}
