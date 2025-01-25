package proxy

import (
	"github.com/grepplabs/kafka-proxy/config"
	"sync/atomic"
)

const UnknownBrokerID = -1

type ListenerConfig struct {
	BrokerAddressPtr  atomic.Pointer[string]
	ListenerAddress   string
	AdvertisedAddress string
	BrokerID          int32
}

func FromListenerConfig(listenerConfig config.ListenerConfig) *ListenerConfig {
	c := &ListenerConfig{
		ListenerAddress:   listenerConfig.ListenerAddress,
		AdvertisedAddress: listenerConfig.AdvertisedAddress,
		BrokerID:          UnknownBrokerID,
	}
	c.BrokerAddressPtr.Store(&listenerConfig.BrokerAddress)
	return c
}

func NewListenerConfig(brokerAddress, listenerAddress, advertisedAddress string, brokerID int32) *ListenerConfig {
	c := &ListenerConfig{
		ListenerAddress:   listenerAddress,
		AdvertisedAddress: advertisedAddress,
		BrokerID:          brokerID,
	}
	c.BrokerAddressPtr.Store(&brokerAddress)
	return c
}
func (c *ListenerConfig) ToListenerConfig() config.ListenerConfig {
	return config.ListenerConfig{
		BrokerAddress:     c.GetBrokerAddress(),
		ListenerAddress:   c.ListenerAddress,
		AdvertisedAddress: c.AdvertisedAddress,
	}
}

func (c *ListenerConfig) GetBrokerAddress() string {
	addressPtr := c.BrokerAddressPtr.Load()
	if addressPtr == nil {
		return ""
	}
	return *addressPtr
}

func (c *ListenerConfig) SetBrokerAddress(address string) {
	c.BrokerAddressPtr.Store(&address)
}
