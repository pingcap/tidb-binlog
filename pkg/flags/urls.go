package flags

import (
	"fmt"
	"github.com/iamxy/tidb-binlog/pkg/types"
	"strings"
)

type URLsValue types.URLs

// Set parses a command line set of URLs formatted like:
// http://127.0.0.1:2380,http://10.1.1.2:80
func (us *URLsValue) Set(s string) error {
	strs := strings.Split(s, ",")
	nus, err := types.NewURLs(strs)
	if err != nil {
		return err
	}

	*us = URLsValue(nus)
	return nil
}

func (us *URLsValue) String() string {
	all := make([]string, len(*us))
	for i, u := range *us {
		all[i] = u.String()
	}
	return strings.Join(all, ",")
}

func NewURLsValue(init string) *URLsValue {
	v := &URLsValue{}
	if err := v.Set(init); err != nil {
		panic(fmt.Sprintf("new URLsValue should never fail: %v", err))
	}
	return v
}
