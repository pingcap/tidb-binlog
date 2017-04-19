package pump

import (
	"testing"

	"github.com/coreos/etcd/integration"
	. "github.com/pingcap/check"
)

var testEtcdCluster *integration.ClusterV3

func TestPump(t *testing.T) {
	testEtcdCluster = integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	TestingT(t)
}

var _ = Suite(&testPumpServerSuite{})

type testPumpServerSuite struct{}
