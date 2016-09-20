package flags

import (
	"strings"
	"net/url"

	"github.com/juju/errors"
	"github.com/pingcap/tidb-binlog/pkg/types"
)

type URLsValue types.URLs

// Set parses a command line set of URLs formatted like:
// http://127.0.0.1:2380,http://10.1.1.2:80
func (us *URLsValue) Set(s string) error {
	strs := strings.Split(s, ",")
	nus, err := types.NewURLs(strs)
	if err != nil {
		return errors.Trace(err)
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

func (us *URLsValue) StringSlice() []string {
	all := make([]string, len(*us))
	for i, u := range *us {
		all[i] = u.String()
	}
	return all
}

func (us *URLsValue) URLSlice() []url.URL {
	urls := []url.URL(*us)
	return urls
}

func NewURLsValue(init string) (*URLsValue, error) {
	v := &URLsValue{}
	err := v.Set(init)
	return v, err
}
