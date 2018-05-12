package drainer

type bitmap struct {
	value   []uint8
	total   int
	current int
}

func newBitmap(total int) *bitmap {
	return &bitmap{
		total: total,
	}
}

func (b *bitmap) set(index int) bool {
	return true
}

func (b *bitmap) completed() bool {
	return true
}
