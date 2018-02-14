package proxy

import (
	"fmt"
	"log"
	"net"
	"sync"
)

type Listeners struct {
	// Source of new connections to Kafka broker.
	connSrc chan Conn
	// listen IP for dynamically start
	defaultListenIP string

	brokerToListenAddresses map[string]string
	lock                    sync.RWMutex
}

func NewListeners(defaultListenIP string) *Listeners {
	return &Listeners{
		defaultListenIP:         defaultListenIP,
		connSrc:                 make(chan Conn, 1),
		brokerToListenAddresses: make(map[string]string),
	}
}

func (p *Listeners) GetNetAddressMapping(brokerHost string, brokerPort int32) (listenHost string, listenPort int32, err error) {
	brokerAddress := net.JoinHostPort(brokerHost, fmt.Sprint(brokerPort))

	p.lock.RLock()
	listenAddress := p.brokerToListenAddresses[brokerAddress]
	p.lock.RUnlock()

	if listenAddress != "" {
		return SplitHostPort(listenAddress)
	}
	return p.ListenDynamicInstance(brokerAddress)
}

func (p *Listeners) ListenDynamicInstance(brokerAddress string) (string, int32, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	// double check
	if v := p.brokerToListenAddresses[brokerAddress]; v != "" {
		return v, 0, nil
	}

	defaultListenAddress := net.JoinHostPort(p.defaultListenIP, fmt.Sprint(0))

	cfg := ListenConfig{ListenAddress: defaultListenAddress, BrokerAddress: brokerAddress}
	l, err := listenInstance(p.connSrc, cfg)
	if err != nil {
		return "", 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	p.brokerToListenAddresses[brokerAddress] = net.JoinHostPort(p.defaultListenIP, fmt.Sprint(port))
	return p.defaultListenIP, int32(port), nil
}

func (p *Listeners) ListenInstances(cfgs []ListenConfig) (<-chan Conn, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// allows multiple local addresses to point to the remote
	for _, v := range cfgs {
		_, err := listenInstance(p.connSrc, v)
		if err != nil {
			return nil, err
		}
		p.brokerToListenAddresses[v.BrokerAddress] = v.ListenAddress
	}
	return p.connSrc, nil
}

func listenInstance(dst chan<- Conn, cfg ListenConfig) (net.Listener, error) {
	l, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				log.Printf("Error in accept for %q on %v: %v", cfg, cfg.ListenAddress, err)
				l.Close()
				return
			}
			log.Printf("New connection for %q", cfg.BrokerAddress)
			dst <- Conn{BrokerAddress: cfg.BrokerAddress, LocalConnection: c}
		}
	}()

	log.Printf("Listening on %s (%s) for %s", cfg.ListenAddress, l.Addr().String(), cfg.BrokerAddress)
	return l, nil
}

type ListenConfig struct {
	BrokerAddress string
	ListenAddress string
}
