package proxy

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"github.com/grepplabs/kafka-proxy/config"
	"github.com/grepplabs/kafka-proxy/pkg/libs/util"
	"github.com/sirupsen/logrus"
)

type ListenFunc func(cfg config.ListenerConfig) (l net.Listener, err error)

type Listeners struct {
	// Source of new connections to Kafka broker.
	connSrc chan Conn
	// listen IP for dynamically start
	defaultListenerIP string
	// advertised address for dynamic listeners
	dynamicAdvertisedListener string
	// socket TCP options
	tcpConnOptions TCPConnOptions

	listenFunc ListenFunc

	disableDynamicListeners  bool
	dynamicSequentialMinPort int

	brokerToListenerConfig map[string]config.ListenerConfig
	lock                   sync.RWMutex
}

func NewListeners(cfg *config.Config) (*Listeners, error) {

	defaultListenerIP := cfg.Proxy.DefaultListenerIP
	dynamicAdvertisedListener := cfg.Proxy.DynamicAdvertisedListener

	tcpConnOptions := TCPConnOptions{
		KeepAlive:       cfg.Proxy.ListenerKeepAlive,
		ReadBufferSize:  cfg.Proxy.ListenerReadBufferSize,
		WriteBufferSize: cfg.Proxy.ListenerWriteBufferSize,
	}

	var tlsConfig *tls.Config
	if cfg.Proxy.TLS.Enable {
		var err error
		tlsConfig, err = newTLSListenerConfig(cfg)
		if err != nil {
			return nil, err
		}
	}

	listenFunc := func(cfg config.ListenerConfig) (net.Listener, error) {
		if tlsConfig != nil {
			return tls.Listen("tcp", cfg.ListenerAddress, tlsConfig)
		}
		return net.Listen("tcp", cfg.ListenerAddress)
	}

	brokerToListenerConfig, err := getBrokerToListenerConfig(cfg)
	if err != nil {
		return nil, err
	}

	return &Listeners{
		defaultListenerIP:         defaultListenerIP,
		dynamicAdvertisedListener: dynamicAdvertisedListener,
		connSrc:                   make(chan Conn, 1),
		brokerToListenerConfig:    brokerToListenerConfig,
		tcpConnOptions:            tcpConnOptions,
		listenFunc:                listenFunc,
		disableDynamicListeners:   cfg.Proxy.DisableDynamicListeners,
		dynamicSequentialMinPort:  cfg.Proxy.DynamicSequentialMinPort,
	}, nil
}

func getBrokerToListenerConfig(cfg *config.Config) (map[string]config.ListenerConfig, error) {
	brokerToListenerConfig := make(map[string]config.ListenerConfig)

	for _, v := range cfg.Proxy.BootstrapServers {
		if lc, ok := brokerToListenerConfig[v.BrokerAddress]; ok {
			if lc.ListenerAddress != v.ListenerAddress || lc.AdvertisedAddress != v.AdvertisedAddress {
				return nil, fmt.Errorf("bootstrap server mapping %s configured twice: %v and %v", v.BrokerAddress, v, lc)
			}
			continue
		}
		logrus.Infof("Bootstrap server %s advertised as %s", v.BrokerAddress, v.AdvertisedAddress)
		brokerToListenerConfig[v.BrokerAddress] = v
	}

	externalToListenerConfig := make(map[string]config.ListenerConfig)
	for _, v := range cfg.Proxy.ExternalServers {
		if lc, ok := externalToListenerConfig[v.BrokerAddress]; ok {
			if lc.ListenerAddress != v.ListenerAddress {
				return nil, fmt.Errorf("external server mapping %s configured twice: %s and %v", v.BrokerAddress, v.ListenerAddress, lc)
			}
			continue
		}
		if v.ListenerAddress != v.AdvertisedAddress {
			return nil, fmt.Errorf("external server mapping has different listener and advertised addresses %v", v)
		}
		externalToListenerConfig[v.BrokerAddress] = v
	}

	for _, v := range externalToListenerConfig {
		if lc, ok := brokerToListenerConfig[v.BrokerAddress]; ok {
			if lc.AdvertisedAddress != v.AdvertisedAddress {
				return nil, fmt.Errorf("bootstrap and external server mappings %s with different advertised addresses: %v and %v", v.BrokerAddress, v.ListenerAddress, lc.AdvertisedAddress)
			}
			continue
		}
		logrus.Infof("External server %s advertised as %s", v.BrokerAddress, v.AdvertisedAddress)
		brokerToListenerConfig[v.BrokerAddress] = v
	}
	return brokerToListenerConfig, nil
}

func (p *Listeners) GetNetAddressMapping(brokerHost string, brokerPort int32) (listenerHost string, listenerPort int32, err error) {
	if brokerHost == "" || brokerPort <= 0 {
		return "", 0, fmt.Errorf("broker address '%s:%d' is invalid", brokerHost, brokerPort)
	}

	brokerAddress := net.JoinHostPort(brokerHost, fmt.Sprint(brokerPort))

	p.lock.RLock()
	listenerConfig, ok := p.brokerToListenerConfig[brokerAddress]
	p.lock.RUnlock()

	if ok {
		logrus.Debugf("Address mappings broker=%s, listener=%s, advertised=%s", listenerConfig.BrokerAddress, listenerConfig.ListenerAddress, listenerConfig.AdvertisedAddress)
		return util.SplitHostPort(listenerConfig.AdvertisedAddress)
	}
	if !p.disableDynamicListeners {
		logrus.Infof("Starting dynamic listener for broker %s", brokerAddress)
		return p.ListenDynamicInstance(brokerAddress)
	}
	return "", 0, fmt.Errorf("net address mapping for %s:%d was not found", brokerHost, brokerPort)
}

func (p *Listeners) ListenDynamicInstance(brokerAddress string) (string, int32, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	// double check
	if v, ok := p.brokerToListenerConfig[brokerAddress]; ok {
		return util.SplitHostPort(v.AdvertisedAddress)
	}

	defaultListenerAddress := net.JoinHostPort(p.defaultListenerIP, fmt.Sprint(p.dynamicSequentialMinPort))
	if p.dynamicSequentialMinPort != 0 {
		p.dynamicSequentialMinPort += 1
	}

	cfg := config.ListenerConfig{ListenerAddress: defaultListenerAddress, BrokerAddress: brokerAddress}
	l, err := listenInstance(p.connSrc, cfg, p.tcpConnOptions, p.listenFunc)
	if err != nil {
		return "", 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	address := net.JoinHostPort(p.defaultListenerIP, fmt.Sprint(port))

	dynamicAdvertisedListener := p.dynamicAdvertisedListener
	if dynamicAdvertisedListener == "" {
		dynamicAdvertisedListener = p.defaultListenerIP
	}

	advertisedAddress := net.JoinHostPort(dynamicAdvertisedListener, fmt.Sprint(port))
	p.brokerToListenerConfig[brokerAddress] = config.ListenerConfig{BrokerAddress: brokerAddress, ListenerAddress: address, AdvertisedAddress: advertisedAddress}

	logrus.Infof("Dynamic listener %s for broker %s advertised as %s", address, brokerAddress, advertisedAddress)

	return dynamicAdvertisedListener, int32(port), nil
}

func (p *Listeners) ListenInstances(cfgs []config.ListenerConfig) (<-chan Conn, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// allows multiple local addresses to point to the remote
	for _, v := range cfgs {
		_, err := listenInstance(p.connSrc, v, p.tcpConnOptions, p.listenFunc)
		if err != nil {
			return nil, err
		}
	}
	return p.connSrc, nil
}

func listenInstance(dst chan<- Conn, cfg config.ListenerConfig, opts TCPConnOptions, listenFunc ListenFunc) (net.Listener, error) {
	l, err := listenFunc(cfg)
	if err != nil {
		return nil, err
	}
	go withRecover(func() {
		for {
			c, err := l.Accept()
			if err != nil {
				logrus.Infof("Error in accept for %q on %v: %v", cfg, cfg.ListenerAddress, err)
				l.Close()
				return
			}
			if tcpConn, ok := c.(*net.TCPConn); ok {
				if err := opts.setTCPConnOptions(tcpConn); err != nil {
					logrus.Infof("WARNING: Error while setting TCP options for accepted connection %q on %v: %v", cfg, l.Addr().String(), err)
				}
			}
			logrus.Infof("New connection for %s", cfg.BrokerAddress)
			dst <- Conn{BrokerAddress: cfg.BrokerAddress, LocalConnection: c}
		}
	})

	logrus.Infof("Listening on %s (%s) for remote %s", cfg.ListenerAddress, l.Addr().String(), cfg.BrokerAddress)
	return l, nil
}
