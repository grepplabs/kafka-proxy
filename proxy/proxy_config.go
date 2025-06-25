package proxy

import (
	"fmt"
	"sync/atomic"

	"github.com/grepplabs/kafka-proxy/config"
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

func (c *ListenerConfig) GetListenerAddress() string {
	return c.ListenerAddress
}

func (c *ListenerConfig) GetBrokerID() int32 {
	return c.BrokerID
}

func (c *ListenerConfig) GetAdvertisedAddress() string {
	return c.AdvertisedAddress
}

func (c *ListenerConfig) SetBrokerAddress(address string) {
	c.BrokerAddressPtr.Store(&address)
}

func (c *ListenerConfig) String() string {
	return fmt.Sprintf("ListenerConfig[%d]: %s->%s->%s", c.BrokerID, c.AdvertisedAddress, c.ListenerAddress, c.GetBrokerAddress())
}
