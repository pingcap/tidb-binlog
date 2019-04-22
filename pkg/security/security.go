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
func (c *Config) ToTLSConfig() (*tls.Config, error) {
	var tlsConfig *tls.Config
	if len(c.SSLCA) != 0 {
		var certificates = make([]tls.Certificate, 0)
		if len(c.SSLCert) != 0 && len(c.SSLKey) != 0 {
			// Load the client certificates from disk
			certificate, err := tls.LoadX509KeyPair(c.SSLCert, c.SSLKey)
			if err != nil {
				return nil, errors.Errorf("could not load client key pair: %s", err)
			}
			certificates = append(certificates, certificate)
		}

		// Create a certificate pool from the certificate authority
		certPool := x509.NewCertPool()
		ca, err := ioutil.ReadFile(c.SSLCA)
		if err != nil {
			return nil, errors.Errorf("could not read ca certificate: %s", err)
		}

		// Append the certificates from the CA
		if !certPool.AppendCertsFromPEM(ca) {
			return nil, errors.New("failed to append ca certs")
		}

		tlsConfig = &tls.Config{
			Certificates: certificates,
			RootCAs:      certPool,
		}
	}

	return tlsConfig, nil
}

// ToTiDBSecurityConfig generates tidb security config
func (c *Config) ToTiDBSecurityConfig() config.Security {
	security := config.Security{
		ClusterSSLCA:   c.SSLCA,
		ClusterSSLCert: c.SSLCert,
		ClusterSSLKey:  c.SSLKey,
	}

	config.GetGlobalConfig().Security = security
	return security
}
