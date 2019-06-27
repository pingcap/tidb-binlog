package column

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/zanmato1984/clickhouse/lib/binary"
)

// Decimal is the Clickhouse Decimal column.
type Decimal struct {
	base
	Precision int
	Scale     int
}

func (d *Decimal) Read(decoder *binary.Decoder) (interface{}, error) {
	return nil, errors.New("shouldn't be reading Decimal values")
}

func (d *Decimal) Write(encoder *binary.Encoder, v interface{}) error {
	switch value := v.(type) {
	case []byte:
		_, err := encoder.Write(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return &ErrUnexpectedType{
			T:      v,
			Column: d,
		}
	}
}

func parseDecimal(name, chType string) (*Decimal, error) {
	if !strings.HasPrefix(chType, "Decimal(") {
		return nil, fmt.Errorf("invalid Decimal format: %s", chType)
	}
	s := chType[8 : len(chType)-1]
	splits := strings.Split(s, ",")
	precision, err := strconv.Atoi(splits[0])
	if err != nil {
		return nil, err
	}
	scale, err := strconv.Atoi(splits[1])
	if err != nil {
		return nil, err
	}
	return &Decimal{
		base: base{
			name:    name,
			chType:  chType,
			valueOf: reflect.ValueOf(make([]byte, 64)),
		},
		Precision: precision,
		Scale:     scale,
	}, nil
}
