package translator

// Hack: as CH date type is too narrow, map it to Int32 instead. Computing should take care of the calculation.
func hackDateTypeMapping() string {
	return "Int32"
}

// Hack: store Date using Int32 as days since 1970-01-01 Int32, CH driver accepts int64 for Int32 type, so leave it int64.
func hackDateData(unix int64) int64 {
	return unix / 24 / 3600
}
