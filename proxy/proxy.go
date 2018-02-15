package proxy

import (
	"fmt"
	"github.com/grepplabs/kafka-proxy/config"
	"log"
	"net"
	"sync"
)

type Listeners struct {
	// Source of new connections to Kafka broker.
	connSrc chan Conn
	// listen IP for dynamically start
	defaultListenerIP string

	brokerToListenerAddresses map[string]string
	lock                      sync.RWMutex
}

func NewListeners(defaultListenerIP string) *Listeners {
	return &Listeners{
		defaultListenerIP:         defaultListenerIP,
		connSrc:                   make(chan Conn, 1),
		brokerToListenerAddresses: make(map[string]string),
	}
}

func (p *Listeners) GetNetAddressMapping(brokerHost string, brokerPort int32) (listenerHost string, listenerPort int32, err error) {
	if brokerHost == "" || brokerPort <= 0 {
		return "", 0, fmt.Errorf("broker address '%s:%d' is invalid", brokerHost, brokerPort)
	}

	brokerAddress := net.JoinHostPort(brokerHost, fmt.Sprint(brokerPort))

	p.lock.RLock()
	listenerAddress := p.brokerToListenerAddresses[brokerAddress]
	p.lock.RUnlock()

	if listenerAddress != "" {
		return config.SplitHostPort(listenerAddress)
	}
	return p.ListenDynamicInstance(brokerAddress)
}

func (p *Listeners) ListenDynamicInstance(brokerAddress string) (string, int32, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	// double check
	if v := p.brokerToListenerAddresses[brokerAddress]; v != "" {
		return v, 0, nil
	}

	defaultListenerAddress := net.JoinHostPort(p.defaultListenerIP, fmt.Sprint(0))

	cfg := config.ListenerConfig{ListenerAddress: defaultListenerAddress, BrokerAddress: brokerAddress}
	l, err := listenInstance(p.connSrc, cfg)
	if err != nil {
		return "", 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	p.brokerToListenerAddresses[brokerAddress] = net.JoinHostPort(p.defaultListenerIP, fmt.Sprint(port))
	return p.defaultListenerIP, int32(port), nil
}

func (p *Listeners) ListenInstances(cfgs []config.ListenerConfig) (<-chan Conn, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// allows multiple local addresses to point to the remote
	for _, v := range cfgs {
		_, err := listenInstance(p.connSrc, v)
		if err != nil {
			return nil, err
		}
		p.brokerToListenerAddresses[v.BrokerAddress] = v.ListenerAddress
	}
	return p.connSrc, nil
}

func listenInstance(dst chan<- Conn, cfg config.ListenerConfig) (net.Listener, error) {
	l, err := net.Listen("tcp", cfg.ListenerAddress)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				log.Printf("Error in accept for %q on %v: %v", cfg, cfg.ListenerAddress, err)
				l.Close()
				return
			}
			log.Printf("New connection for %q", cfg.BrokerAddress)
			dst <- Conn{BrokerAddress: cfg.BrokerAddress, LocalConnection: c}
		}
	}()

	log.Printf("Listening on %s (%s) for %s", cfg.ListenerAddress, l.Addr().String(), cfg.BrokerAddress)
	return l, nil
}
