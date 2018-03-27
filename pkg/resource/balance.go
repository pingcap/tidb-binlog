package resource

// BalanceResource balances the resource
func BalanceResource(total, used uint64, useMap map[string]uint64, average bool) map[string]uint64 {
	maxMap := make(map[string]uint64)

	if len(useMap) == 0 {
		return maxMap
	}

	// use basicResource avoid some label's max resource is too small
	labelNum := uint64(len(useMap))
	basicResource := total / labelNum / 2

	for label, resource := range useMap {
		if average {
			maxMap[label] = total / labelNum
		} else {
			maxMap[label] = basicResource + total/2*(resource/used)
		}
	}

	return maxMap
}
