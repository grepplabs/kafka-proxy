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
					AdvertisedAddress: "kafka-proxy-0:32404",
				},
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
					AdvertisedAddress: "kafka-proxy-0:32400"},
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
		brokerToListenerConfig, err := getBrokerToListenerConfig(c)
		a.Equal(tt.err, err)

		mapping := make(map[string]config.ListenerConfig)
		for k, v := range brokerToListenerConfig {
			mapping[k] = config.ListenerConfig{
				BrokerAddress:     v.GetBrokerAddress(),
				ListenerAddress:   v.ListenerAddress,
				AdvertisedAddress: v.AdvertisedAddress,
			}
		}
		assert.ObjectsAreEqual(tt.mapping, mapping)
	}
}

func TestGetDynamicAdvertisedAddress(t *testing.T) {
	tests := []struct {
		name                      string
		dynamicAdvertisedListener string
		defaultListenerIP         string
		brokerID                  int32
		port                      int
		expectedHost              string
		expectedPort              int
		expectError               bool
	}{
		{
			name:                      "Default listener IP is 127.0.0.1",
			dynamicAdvertisedListener: "",
			defaultListenerIP:         "127.0.0.1",
			brokerID:                  1,
			port:                      9092,
			expectedHost:              "127.0.0.1",
			expectedPort:              9092,
			expectError:               false,
		},
		{
			name:                      "Default listener IP is 0.0.0.0",
			dynamicAdvertisedListener: "",
			defaultListenerIP:         "0.0.0.0",
			brokerID:                  1,
			port:                      9092,
			expectedHost:              "0.0.0.0",
			expectedPort:              9092,
			expectError:               false,
		},
		{
			name:                      "Default listener IP is localhost",
			dynamicAdvertisedListener: "",
			defaultListenerIP:         "localhost",
			brokerID:                  1,
			port:                      9092,
			expectedHost:              "localhost",
			expectedPort:              9092,
			expectError:               false,
		},
		{
			name:                      "Dynamic listener no template, host is IP",
			dynamicAdvertisedListener: "0.0.0.0",
			defaultListenerIP:         "127.0.0.1",
			brokerID:                  2,
			port:                      9093,
			expectedHost:              "0.0.0.0",
			expectedPort:              9093,
			expectError:               false,
		},
		{
			name:                      "Dynamic listener no template, host is IP and port is provided",
			dynamicAdvertisedListener: "0.0.0.0:30000",
			defaultListenerIP:         "127.0.0.1",
			brokerID:                  2,
			port:                      9093,
			expectedHost:              "0.0.0.0",
			expectedPort:              30000,
			expectError:               false,
		},
		{
			name:                      "Dynamic listener no template, host is dns name",
			dynamicAdvertisedListener: "kafka-proxy.provisionedmskclust.zgjvgc.c2.kafka.eu-central-1.amazonaws.com",
			defaultListenerIP:         "127.0.0.1",
			brokerID:                  2,
			port:                      9093,
			expectedHost:              "kafka-proxy.provisionedmskclust.zgjvgc.c2.kafka.eu-central-1.amazonaws.com",
			expectedPort:              9093,
			expectError:               false,
		},
		{
			name:                      "Dynamic listener no template, host is dns name and port is provided",
			dynamicAdvertisedListener: "kafka-proxy.grepplabs.com:30000",
			defaultListenerIP:         "127.0.0.1",
			brokerID:                  2,
			port:                      9093,
			expectedHost:              "kafka-proxy.grepplabs.com",
			expectedPort:              30000,
			expectError:               false,
		},
		{
			name:                      "Dynamic listener with template",
			dynamicAdvertisedListener: "b-{{.brokerId}}.provisionedmskclust.zgjvgc.c2.kafka.eu-central-1.amazonaws.com",
			defaultListenerIP:         "127.0.0.1",
			brokerID:                  2,
			port:                      9093,
			expectedHost:              "b-2.provisionedmskclust.zgjvgc.c2.kafka.eu-central-1.amazonaws.com",
			expectedPort:              9093,
			expectError:               false,
		},
		{
			name:                      "Dynamic listener with template and port is provided",
			dynamicAdvertisedListener: "b-{{.brokerId}}.provisionedmskclust.zgjvgc.c2.kafka.eu-central-1.amazonaws.com:30000",
			defaultListenerIP:         "127.0.0.1",
			brokerID:                  2,
			port:                      9093,
			expectedHost:              "b-2.provisionedmskclust.zgjvgc.c2.kafka.eu-central-1.amazonaws.com",
			expectedPort:              30000,
			expectError:               false,
		},
		{
			name:                      "Invalid dynamic listener template",
			dynamicAdvertisedListener: "broker-{{.invalid}}",
			defaultListenerIP:         "127.0.0.1",
			brokerID:                  3,
			port:                      9094,
			expectedHost:              "",
			expectedPort:              0,
			expectError:               true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			listeners := &Listeners{
				dynamicAdvertisedListener: tt.dynamicAdvertisedListener,
				defaultListenerIP:         tt.defaultListenerIP,
			}

			host, port, err := listeners.getDynamicAdvertisedAddress(tt.brokerID, tt.port)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedHost, host)
				assert.Equal(t, tt.expectedPort, port)
			}
		})
	}
}
