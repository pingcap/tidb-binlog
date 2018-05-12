package drainer

// unsupported concurrency
type bitmap struct {
	value   []uint8
	total   int
	current int
}

func newBitmap(total int) *bitmap {
	if total == 0 {
		return nil
	}

	bm := &bitmap{
		value: make([]uint8, (total+7)/8),
		total: total,
	}

	// mask useless bit
	bm.value[len(bm.value)-1] = 0xFF ^ (1<<uint(total%8) - 1)
	return bm
}

func (b *bitmap) set(index int) {
	mask := uint8(1 << uint(index%8))
	bucket := b.value[index/8]

	if bucket|mask != bucket {
		b.value[index/8] = bucket | mask
		b.current++
	}
}

func (b *bitmap) completed() bool {
	return b.current == b.total
}
