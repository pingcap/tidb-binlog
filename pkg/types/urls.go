package types

import (
	"net"
	"net/url"
	"sort"
	"strings"

	"github.com/pingcap/errors"
)

// URLs defines a slice of URLs as a type
type URLs []url.URL

// NewURLs return a URLs from a slice of formatted URL strings
func NewURLs(strs []string) (URLs, error) {
	all := make([]url.URL, len(strs))
	if len(all) == 0 {
		return nil, errors.New("no valid URLs given")
	}
	for i, in := range strs {
		in = strings.TrimSpace(in)
		u, err := url.Parse(in)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "unix" && u.Scheme != "unixs" {
			return nil, errors.Errorf("URL scheme must be http, https, unix, or unixs: %s", in)
		}
		if _, _, err := net.SplitHostPort(u.Host); err != nil {
			return nil, errors.Errorf(`URL address does not have the form "host:port": %s`, in)
		}
		if u.Path != "" {
			return nil, errors.Errorf("URL must not contain a path: %s", in)
		}
		all[i] = *u
	}
	us := URLs(all)
	sort.Slice(us, func(i, j int) bool { return us[i].String() < us[j].String() })

	return us, nil
}

// String return a string of list of URLs witch separated by comma
func (us URLs) String() string {
	return strings.Join(us.StringSlice(), ",")
}

// StringSlice return a slice of formatted string of URL
func (us URLs) StringSlice() []string {
	out := make([]string, len(us))
	for i := range us {
		out[i] = us[i].String()
	}

	return out
}
