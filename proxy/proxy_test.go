package proxy

import (
	"fmt"
	"testing"

	"github.com/grepplabs/kafka-proxy/config"
	"github.com/stretchr/testify/assert"
)

func TestGetBrokerToListenerConfig(t *testing.T) {
	a := assert.New(t)

	tests := []struct {
		bootstrapServers []config.ListenerConfig
		externalServers  []config.ListenerConfig
		err              error
		mapping          map[string]config.ListenerConfig
	}{
		{
			[]config.ListenerConfig{},
			[]config.ListenerConfig{},
			nil,
			map[string]config.ListenerConfig{},
		},
		{
			[]config.ListenerConfig{
				{
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "0.0.0.0:32400",
					AdvertisedAddress: "0.0.0.0:32400",
				},
			},
			[]config.ListenerConfig{},
			nil,
			map[string]config.ListenerConfig{
				"192.168.99.100:32400": {
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "0.0.0.0:32400",
					AdvertisedAddress: "0.0.0.0:32400",
				},
			},
		},
		{
			[]config.ListenerConfig{
				{
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "0.0.0.0:32400",
					AdvertisedAddress: "kafka-proxy-0:32400",
				},
				{
					BrokerAddress:     "192.168.99.100:32401",
					ListenerAddress:   "0.0.0.0:32401",
					AdvertisedAddress: "kafka-proxy-0:32401",
				},
				{
					BrokerAddress:     "192.168.99.100:32402",
					ListenerAddress:   "0.0.0.0:32402",
					AdvertisedAddress: "kafka-proxy-0:32402",
				},
			},
			[]config.ListenerConfig{},
			nil,
			map[string]config.ListenerConfig{
				"192.168.99.100:32400": {
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "0.0.0.0:32400",
					AdvertisedAddress: "kafka-proxy-0:32400",
				},
				"192.168.99.100:32401": {
					BrokerAddress:     "192.168.99.100:32401",
					ListenerAddress:   "0.0.0.0:32401",
					AdvertisedAddress: "kafka-proxy-0:32401",
				},
				"192.168.99.100:32402": {
					BrokerAddress:     "192.168.99.100:32402",
					ListenerAddress:   "0.0.0.0:32402",
					AdvertisedAddress: "kafka-proxy-0:32402",
				},
			},
		},
		{
			[]config.ListenerConfig{
				{
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "0.0.0.0:32400",
					AdvertisedAddress: "0.0.0.0:32400",
				},
				{
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "0.0.0.0:32400",
					AdvertisedAddress: "0.0.0.0:32400",
				},
			},
			[]config.ListenerConfig{},
			nil,
			map[string]config.ListenerConfig{
				"192.168.99.100:32400": {
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "0.0.0.0:32400",
					AdvertisedAddress: "0.0.0.0:32400",
				},
			},
		},
		{
			[]config.ListenerConfig{
				{
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "0.0.0.0:32400",
					AdvertisedAddress: "0.0.0.0:32400",
				},
				{
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "0.0.0.0:32401",
					AdvertisedAddress: "0.0.0.0:32400",
				},
			},
			[]config.ListenerConfig{},
			fmt.Errorf("bootstrap server mapping 192.168.99.100:32400 configured twice: {192.168.99.100:32400 0.0.0.0:32401 0.0.0.0:32400} and {192.168.99.100:32400 0.0.0.0:32400 0.0.0.0:32400}"),
			nil,
		},
		{
			[]config.ListenerConfig{
				{
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "0.0.0.0:32400",
					AdvertisedAddress: "0.0.0.0:32400",
				},
				{
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "0.0.0.0:32400",
					AdvertisedAddress: "0.0.0.0:32401",
				},
			},
			[]config.ListenerConfig{},
			fmt.Errorf("bootstrap server mapping 192.168.99.100:32400 configured twice: {192.168.99.100:32400 0.0.0.0:32400 0.0.0.0:32401} and {192.168.99.100:32400 0.0.0.0:32400 0.0.0.0:32400}"),
			nil,
		},
		{
			[]config.ListenerConfig{
				{
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "0.0.0.0:32400",
					AdvertisedAddress: "kafka-proxy-0:32400",
				},
				{
					BrokerAddress:     "192.168.99.100:32401",
					ListenerAddress:   "0.0.0.0:32401",
					AdvertisedAddress: "kafka-proxy-0:32401",
				},
				{
					BrokerAddress:     "192.168.99.100:32402",
					ListenerAddress:   "0.0.0.0:32402",
					AdvertisedAddress: "kafka-proxy-0:32402",
				},
			},
			[]config.ListenerConfig{
				{
					BrokerAddress:     "192.168.99.100:32403",
					ListenerAddress:   "kafka-proxy-0:32403",
					AdvertisedAddress: "kafka-proxy-0:32403"},
				{
					BrokerAddress:     "192.168.99.100:32404",
					ListenerAddress:   "kafka-proxy-0:32404",
					AdvertisedAddress: "kafka-proxy-0:32404"},
			},
			nil,
			map[string]config.ListenerConfig{
				"192.168.99.100:32400": {
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "0.0.0.0:32400",
					AdvertisedAddress: "kafka-proxy-0:32400",
				},
				"192.168.99.100:32401": {
					BrokerAddress:     "192.168.99.100:32401",
					ListenerAddress:   "0.0.0.0:32401",
					AdvertisedAddress: "kafka-proxy-0:32401",
				},
				"192.168.99.100:32402": {
					BrokerAddress:     "192.168.99.100:32402",
					ListenerAddress:   "0.0.0.0:32402",
					AdvertisedAddress: "kafka-proxy-0:32402",
				},
				"192.168.99.100:32403": {
					BrokerAddress:     "192.168.99.100:32403",
					ListenerAddress:   "kafka-proxy-0:32403",
					AdvertisedAddress: "kafka-proxy-0:32403",
				},
				"192.168.99.100:32404": {
					BrokerAddress:     "192.168.99.100:32404",
					ListenerAddress:   "kafka-proxy-0:32404",
					AdvertisedAddress: "kafka-proxy-0:32404",
				},
			},
		},
		{
			[]config.ListenerConfig{
				{
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "0.0.0.0:32400",
					AdvertisedAddress: "kafka-proxy-0:32400",
				},
			},
			[]config.ListenerConfig{
				{
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "kafka-proxy-0:32400",
					AdvertisedAddress: "kafka-proxy-0:32400",
				},
			},
			nil,
			map[string]config.ListenerConfig{
				"192.168.99.100:32400": {
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "0.0.0.0:32400",
					AdvertisedAddress: "kafka-proxy-0:32400",
				},
			},
		},
		{
			[]config.ListenerConfig{
				{
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "0.0.0.0:32400",
					AdvertisedAddress: "kafka-proxy-0:32400",
				},
			},
			[]config.ListenerConfig{
				{
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "kafka-proxy-1:32400",
					AdvertisedAddress: "kafka-proxy-1:32400",
				},
			},
			fmt.Errorf("bootstrap and external server mappings 192.168.99.100:32400 with different advertised addresses: kafka-proxy-1:32400 and kafka-proxy-0:32400"),
			nil,
		},
		{
			[]config.ListenerConfig{},
			[]config.ListenerConfig{
				{
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "kafka-proxy-0:32400",
					AdvertisedAddress: "kafka-proxy-0:32401",
				},
			},
			fmt.Errorf("external server mapping has different listener and advertised addresses {192.168.99.100:32400 kafka-proxy-0:32400 kafka-proxy-0:32401}"),
			nil,
		},
		{
			[]config.ListenerConfig{},
			[]config.ListenerConfig{
				{
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "kafka-proxy-0:32400",
					AdvertisedAddress: "kafka-proxy-0:32400",
				},
				{
					BrokerAddress:     "192.168.99.100:32400",
					ListenerAddress:   "kafka-proxy-0:32401",
					AdvertisedAddress: "kafka-proxy-0:32401",
				},
			},
			fmt.Errorf("external server mapping 192.168.99.100:32400 configured twice: kafka-proxy-0:32401 and {192.168.99.100:32400 kafka-proxy-0:32400 kafka-proxy-0:32400}"),
			nil,
		},
	}
	for _, tt := range tests {
		c := &config.Config{}
		c.Proxy.BootstrapServers = tt.bootstrapServers
		c.Proxy.ExternalServers = tt.externalServers
		mapping, err := getBrokerToListenerConfig(c)
		a.Equal(tt.err, err)
		a.Equal(tt.mapping, mapping)
	}
}
