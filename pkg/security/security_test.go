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

package security_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"io/ioutil"
	"path/filepath"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb-binlog/pkg/security"
	"github.com/pingcap/tidb/config"
)

func TestClient(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSecuritySuite{})

type testSecuritySuite struct{}

func (s *testSecuritySuite) TestToTiDBSecurityConfig(c *C) {
	dummyConfig := security.Config{
		SSLCA:   "dummy-ca.crt",
		SSLCert: "dummy.crt",
		SSLKey:  "dummy.key",
	}

	globalConfig := config.GetGlobalConfig()
	oldSecurityConfig := globalConfig.Security
	defer func() {
		globalConfig.Security = oldSecurityConfig
	}()

	newSecurityConfig := dummyConfig.ToTiDBSecurityConfig()
	c.Assert(oldSecurityConfig, Not(Equals), newSecurityConfig)
	c.Assert(newSecurityConfig.ClusterSSLCA, Equals, dummyConfig.SSLCA)
	c.Assert(newSecurityConfig.ClusterSSLKey, Equals, dummyConfig.SSLKey)
	c.Assert(newSecurityConfig.ClusterSSLCert, Equals, dummyConfig.SSLCert)
	c.Assert(globalConfig.Security, Equals, newSecurityConfig)
}

func (s *testSecuritySuite) TestToTLSConfig(c *C) {
	temp := c.MkDir()
	dummyConfig := security.Config{
		SSLCA:   filepath.Join(temp, "ca.crt"),
		SSLCert: filepath.Join(temp, "ssl.crt"),
		SSLKey:  filepath.Join(temp, "ssl.key"),
	}

	// These certs are generated with:
	//
	// ```sh
	// # generate CA keys
	// openssl ecparam -name secp224r1 -genkey -noout -out ca.key
	// openssl req -x509 -new -nodes -key ca.key -days 999999 -out ca.crt -subj '/CN=localhost'
	//
	// # generate SSL keys
	// openssl ecparam -name secp224r1 -genkey -noout -out ssl.key
	// openssl req -new -key ssl.key -out ssl.csr -subj '/CN=localhost'
	// openssl x509 -req -in ssl.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out ssl.crt -days 999999
	// ```

	err := ioutil.WriteFile(dummyConfig.SSLCA, []byte(`
-----BEGIN CERTIFICATE-----
MIIBBjCBtQIJAMLMVjQw2v1pMAoGCCqGSM49BAMCMBQxEjAQBgNVBAMMCWxvY2Fs
aG9zdDAgFw0xOTA0MTcxODEyNDNaGA80NzU3MDMxMzE4MTI0M1owFDESMBAGA1UE
AwwJbG9jYWxob3N0ME4wEAYHKoZIzj0CAQYFK4EEACEDOgAELwEHdmAcDtBYK9BH
72q0dKbBBqIG7MZ5+qc+LTcz0OSdhuWkWUZkNN6MqKAPuP7nSo1+21Vb8YswCgYI
KoZIzj0EAwIDQAAwPQIcbNvV16rOOzwotH65cJY6cCdf0h3IODjlWMf1qAIdAIBB
Fma6g8iW5zdQPqDR9BGqugNPjtI/SMK6tfQ=
-----END CERTIFICATE-----
	`), 0644)
	c.Assert(err, IsNil)

	err = ioutil.WriteFile(dummyConfig.SSLCert, []byte(`
-----BEGIN CERTIFICATE-----
MIIBBTCBtAIJAP8wfS+6tJ3LMAkGByqGSM49BAEwFDESMBAGA1UEAwwJbG9jYWxo
b3N0MCAXDTE5MDQxNzE4MTI0NFoYDzQ3NTcwMzEzMTgxMjQ0WjAUMRIwEAYDVQQD
DAlsb2NhbGhvc3QwTjAQBgcqhkjOPQIBBgUrgQQAIQM6AAQJaXEnDhG2tPxD4wl1
ycaZwqWm9JeQZFuUPgxekGwCMM22sKpYLvhdKroSBoKWwXIC6vZMWeIj/zAJBgcq
hkjOPQQBA0EAMD4CHQC05dXi9zFLjYjQGhpJNx+Nc/5vC6E7j/MU+xsTAh0A6SUn
g916djuFWv8djdDq+0NEFD9OzgPdSb8rZw==
-----END CERTIFICATE-----
	`), 0644)
	c.Assert(err, IsNil)

	err = ioutil.WriteFile(dummyConfig.SSLKey, []byte(`
-----BEGIN EC PRIVATE KEY-----
MGgCAQEEHCsPBVueZ3YX3yp1tn15YXj0cTKGCo1SO1EWO92gBwYFK4EEACGhPAM6
AAQJaXEnDhG2tPxD4wl1ycaZwqWm9JeQZFuUPgxekGwCMM22sKpYLvhdKroSBoKW
wXIC6vZMWeIj/w==
-----END EC PRIVATE KEY-----
	`), 0600)
	c.Assert(err, IsNil)

	config, err := dummyConfig.ToTLSConfig()
	c.Assert(err, IsNil)
	c.Assert(config, NotNil)
	c.Assert(config.RootCAs.Subjects(), HasLen, 1)
	c.Assert(config.Certificates, HasLen, 1)
	sslKey, ok := config.Certificates[0].PrivateKey.(*ecdsa.PrivateKey)
	c.Assert(ok, IsTrue)
	c.Assert(sslKey.Curve, Equals, elliptic.P224())
}

func (s *testSecuritySuite) TestEmptyTLSConfig(c *C) {
	var dummyConfig security.Config
	config, err := dummyConfig.ToTLSConfig()
	c.Assert(config, IsNil)
	c.Assert(err, IsNil)
}

func (s *testSecuritySuite) TestInvalidTLSConfig(c *C) {
	temp := c.MkDir()

	dummyConfig := security.Config{
		SSLCA: filepath.Join(temp, "invalid-ca.crt"),
	}
	_, err := dummyConfig.ToTLSConfig()
	c.Assert(err, ErrorMatches, "could not read ca certificate.*")

	err = ioutil.WriteFile(dummyConfig.SSLCA, []byte("invalid certificate"), 0644)
	c.Assert(err, IsNil)

	_, err = dummyConfig.ToTLSConfig()
	c.Assert(err, ErrorMatches, "failed to append ca certs.*")

	dummyConfig.SSLCert = filepath.Join(temp, "invalid-ssl.crt")
	dummyConfig.SSLKey = filepath.Join(temp, "invalid-ssl.key")

	err = ioutil.WriteFile(dummyConfig.SSLCert, []byte("invalid certificate"), 0644)
	c.Assert(err, IsNil)
	err = ioutil.WriteFile(dummyConfig.SSLKey, []byte("invalid key"), 0600)
	c.Assert(err, IsNil)

	_, err = dummyConfig.ToTLSConfig()
	c.Assert(err, ErrorMatches, "could not load client key pair.*")

}
