// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"crypto/tls"
	"net"
	"net/url"

	"github.com/pingcap/errors"
)

// Listen return the listener from tls.Listen if tlsConfig is NOT Nil.
func Listen(network, addr string, tlsConfig *tls.Config) (listener net.Listener, err error) {
	URL, err := url.Parse(addr)
	if err != nil {
		return nil, errors.Annotatef(err, "invalid listening socket addr (%s)", addr)
	}

	if tlsConfig != nil {
		listener, err = tls.Listen(network, URL.Host, tlsConfig)
		if err != nil {
			return nil, errors.Annotatef(err, "fail to start %s on %s", network, URL.Host)
		}
	} else {
		listener, err = net.Listen(network, URL.Host)
		if err != nil {
			return nil, errors.Annotatef(err, "fail to start %s on %s", network, URL.Host)
		}
	}

	return listener, nil
}
