package server

import (
	"github.com/grepplabs/kafka-proxy/config"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func setupBootstrapServersMappingTest() {
	Server.ResetFlags()
	c = new(config.Config)
	initFlags()
	os.Setenv("BOOTSTRAP_SERVER_MAPPING", "")
}

func TestBootstrapServersMappingFromFlags(t *testing.T) {
	setupBootstrapServersMappingTest()

	args := []string{"cobra.test",
		"--bootstrap-server-mapping", "192.168.99.100:32401,0.0.0.0:32401",
		"--bootstrap-server-mapping", "192.168.99.100:32402,0.0.0.0:32402",
		"--bootstrap-server-mapping", "kafka-2.example.com:9092,0.0.0.0:32403,kafka-2.grepplabs.com:9092",
	}

	Server.ParseFlags(args)
	err := Server.PreRunE(nil, args)
	a := assert.New(t)
	a.Nil(err)
	a.Len(c.Proxy.BootstrapServers, 3)

	a.Equal(c.Proxy.BootstrapServers[0].BrokerAddress, "192.168.99.100:32401")
	a.Equal(c.Proxy.BootstrapServers[0].ListenerAddress, "0.0.0.0:32401")
	a.Equal(c.Proxy.BootstrapServers[0].AdvertisedAddress, "0.0.0.0:32401")

	a.Equal(c.Proxy.BootstrapServers[1].BrokerAddress, "192.168.99.100:32402")
	a.Equal(c.Proxy.BootstrapServers[1].ListenerAddress, "0.0.0.0:32402")
	a.Equal(c.Proxy.BootstrapServers[1].AdvertisedAddress, "0.0.0.0:32402")

	a.Equal(c.Proxy.BootstrapServers[2].BrokerAddress, "kafka-2.example.com:9092")
	a.Equal(c.Proxy.BootstrapServers[2].ListenerAddress, "0.0.0.0:32403")
	a.Equal(c.Proxy.BootstrapServers[2].AdvertisedAddress, "kafka-2.grepplabs.com:9092")

}

func TestBootstrapServersMappingFromEnv(t *testing.T) {
	setupBootstrapServersMappingTest()

	os.Setenv("BOOTSTRAP_SERVER_MAPPING", "192.168.99.100:32404,0.0.0.0:32404 kafka-5.example.com:9092,0.0.0.0:32405,kafka-5.grepplabs.com:9092")

	var args []string
	Server.ParseFlags(args)
	err := Server.PreRunE(nil, args)
	a := assert.New(t)
	a.Nil(err)
	a.Len(c.Proxy.BootstrapServers, 2)

	a.Equal(c.Proxy.BootstrapServers[0].BrokerAddress, "192.168.99.100:32404")
	a.Equal(c.Proxy.BootstrapServers[0].ListenerAddress, "0.0.0.0:32404")
	a.Equal(c.Proxy.BootstrapServers[0].AdvertisedAddress, "0.0.0.0:32404")

	a.Equal(c.Proxy.BootstrapServers[1].BrokerAddress, "kafka-5.example.com:9092")
	a.Equal(c.Proxy.BootstrapServers[1].ListenerAddress, "0.0.0.0:32405")
	a.Equal(c.Proxy.BootstrapServers[1].AdvertisedAddress, "kafka-5.grepplabs.com:9092")

}

func TestEmptyBootstrapServersMapping(t *testing.T) {
	setupBootstrapServersMappingTest()

	var args []string
	Server.ParseFlags(args)
	err := Server.PreRunE(nil, args)
	a := assert.New(t)
	a.Error(err, "list of bootstrap-server-mapping must not be empty")
}
