package util

// TsToTimestamp translate ts to timestamp
func TsToTimestamp(ts int64) int64 {
	return ts >> 18 / 1000
}
