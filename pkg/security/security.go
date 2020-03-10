// Copyright 2019 PingCAP, Inc.
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

package security

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
)

// Config is security config
type Config struct {
	SSLCA   string `toml:"ssl-ca" json:"ssl-ca"`
	SSLCert string `toml:"ssl-cert" json:"ssl-cert"`
	SSLKey  string `toml:"ssl-key" json:"ssl-key"`
}

// ToTLSConfig generates tls's config based on security section of the config.
func (c *Config) ToTLSConfig() (tlsConfig *tls.Config, err error) {
	if c.SSLCA == "" {
		return
	}

	// Create a certificate pool from the certificate authority
	certPool := x509.NewCertPool()
	var ca []byte
	ca, err = ioutil.ReadFile(c.SSLCA)
	if err != nil {
		return nil, errors.Errorf("could not read ca certificate: %s", err)
	}

	// Append the certificates from the CA
	if !certPool.AppendCertsFromPEM(ca) {
		return nil, errors.New("failed to append ca certs")
	}

	tlsConfig = &tls.Config{
		RootCAs: certPool,
	}

	if len(c.SSLCert) != 0 && len(c.SSLKey) != 0 {
		getCert := func() (*tls.Certificate, error) {
			// Load the client certificates from disk
			cert, err := tls.LoadX509KeyPair(c.SSLCert, c.SSLKey)
			if err != nil {
				return nil, errors.Errorf("could not load client key pair: %s", err)
			}
			return &cert, nil
		}

		// pre-test cert's loading.
		if _, err = getCert(); err != nil {
			return
		}

		tlsConfig.GetClientCertificate = func(info *tls.CertificateRequestInfo) (certificate *tls.Certificate, err error) {
			return getCert()
		}
		tlsConfig.GetCertificate = func(info *tls.ClientHelloInfo) (certificate *tls.Certificate, err error) {
			return getCert()
		}
	}

	return
}

// ToTiDBSecurityConfig generates tidb security config
func (c *Config) ToTiDBSecurityConfig() config.Security {
	security := config.Security{
		ClusterSSLCA:   c.SSLCA,
		ClusterSSLCert: c.SSLCert,
		ClusterSSLKey:  c.SSLKey,
	}

	// The TiKV client(kvstore.New) we use will use this global var as the TLS config.
	// TODO avoid such magic implicit change when call this func.
	config.GetGlobalConfig().Security = security
	return security
}
