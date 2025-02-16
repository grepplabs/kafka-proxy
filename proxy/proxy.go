package proxy

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"sync"
	"text/template"

	"github.com/grepplabs/kafka-proxy/config"
	"github.com/grepplabs/kafka-proxy/pkg/libs/util"
	"github.com/sirupsen/logrus"
)

type ListenFunc func(cfg *ListenerConfig) (l net.Listener, err error)

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

	deterministicListeners   bool
	disableDynamicListeners  bool
	dynamicSequentialMinPort int

	brokerToListenerConfig map[string]*ListenerConfig
	lock                   sync.RWMutex
}

func NewListeners(cfg *config.Config) (*Listeners, error) {
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

	listenFunc := func(cfg *ListenerConfig) (net.Listener, error) {
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
		defaultListenerIP:         cfg.Proxy.DefaultListenerIP,
		dynamicAdvertisedListener: cfg.Proxy.DynamicAdvertisedListener,
		connSrc:                   make(chan Conn, 1),
		brokerToListenerConfig:    brokerToListenerConfig,
		tcpConnOptions:            tcpConnOptions,
		listenFunc:                listenFunc,
		deterministicListeners:    cfg.Proxy.DeterministicListeners,
		disableDynamicListeners:   cfg.Proxy.DisableDynamicListeners,
		dynamicSequentialMinPort:  cfg.Proxy.DynamicSequentialMinPort,
	}, nil
}

func getBrokerToListenerConfig(cfg *config.Config) (map[string]*ListenerConfig, error) {
	brokerToListenerConfig := make(map[string]*ListenerConfig)

	for _, v := range cfg.Proxy.BootstrapServers {
		if lc, ok := brokerToListenerConfig[v.BrokerAddress]; ok {
			if lc.ListenerAddress != v.ListenerAddress || lc.AdvertisedAddress != v.AdvertisedAddress {
				return nil, fmt.Errorf("bootstrap server mapping %s configured twice: %v and %v", v.BrokerAddress, v, lc.ToListenerConfig())
			}
			continue
		}
		logrus.Infof("Bootstrap server %s advertised as %s", v.BrokerAddress, v.AdvertisedAddress)
		brokerToListenerConfig[v.BrokerAddress] = FromListenerConfig(v)
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
		brokerToListenerConfig[v.BrokerAddress] = FromListenerConfig(v)
	}
	return brokerToListenerConfig, nil
}

func (p *Listeners) GetNetAddressMapping(brokerHost string, brokerPort int32, brokerId int32) (listenerHost string, listenerPort int32, err error) {
	if brokerHost == "" || brokerPort <= 0 {
		return "", 0, fmt.Errorf("broker address '%s:%d' is invalid", brokerHost, brokerPort)
	}

	brokerAddress := net.JoinHostPort(brokerHost, fmt.Sprint(brokerPort))

	p.lock.RLock()
	listenerConfig, ok := p.brokerToListenerConfig[brokerAddress]
	p.lock.RUnlock()

	if ok {
		logrus.Debugf("Address mappings broker=%s, listener=%s, advertised=%s, brokerId=%d", listenerConfig.GetBrokerAddress(), listenerConfig.ListenerAddress, listenerConfig.AdvertisedAddress, brokerId)
		return util.SplitHostPort(listenerConfig.AdvertisedAddress)
	}
	if !p.disableDynamicListeners {
		logrus.Infof("Starting dynamic listener for broker %s", brokerAddress)
		return p.ListenDynamicInstance(brokerAddress, brokerId)
	}
	return "", 0, fmt.Errorf("net address mapping for %s:%d was not found", brokerHost, brokerPort)
}

func (p *Listeners) findListenerConfig(brokerId int32) *ListenerConfig {
	for _, listenerConfig := range p.brokerToListenerConfig {
		if listenerConfig.BrokerID == brokerId {
			return listenerConfig
		}
	}
	return nil
}

func (p *Listeners) ListenDynamicInstance(brokerAddress string, brokerId int32) (string, int32, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	// double check
	if v, ok := p.brokerToListenerConfig[brokerAddress]; ok {
		return util.SplitHostPort(v.AdvertisedAddress)
	}

	var listenerAddress string
	if p.deterministicListeners {
		if brokerId < 0 {
			return "", 0, fmt.Errorf("brokerId is negative %s %d", brokerAddress, brokerId)
		}
		deterministicPort := p.dynamicSequentialMinPort + int(brokerId)
		if deterministicPort < p.dynamicSequentialMinPort {
			return "", 0, fmt.Errorf("port assignment overflow %s %d: %d", brokerAddress, brokerId, deterministicPort)
		}
		listenerAddress = net.JoinHostPort(p.defaultListenerIP, strconv.Itoa(deterministicPort))
		cfg := p.findListenerConfig(brokerId)
		if cfg != nil {
			oldBrokerAddress := cfg.GetBrokerAddress()
			if oldBrokerAddress != brokerAddress {
				delete(p.brokerToListenerConfig, oldBrokerAddress)
				cfg.SetBrokerAddress(brokerAddress)
				p.brokerToListenerConfig[brokerAddress] = cfg
				logrus.Infof("Broker address changed listener %s for new address %s old address %s brokerId %d advertised as %s", cfg.ListenerAddress, cfg.GetBrokerAddress(), oldBrokerAddress, cfg.BrokerID, cfg.AdvertisedAddress)
			}
			return util.SplitHostPort(cfg.AdvertisedAddress)
		}
	} else {
		listenerAddress = net.JoinHostPort(p.defaultListenerIP, strconv.Itoa(p.dynamicSequentialMinPort))
		if p.dynamicSequentialMinPort != 0 {
			p.dynamicSequentialMinPort += 1
		}
	}
	cfg := NewListenerConfig(brokerAddress, listenerAddress, "", brokerId)
	l, err := listenInstance(p.connSrc, cfg, p.tcpConnOptions, p.listenFunc)
	if err != nil {
		return "", 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	address := net.JoinHostPort(p.defaultListenerIP, fmt.Sprint(port))

	dynamicAdvertisedHost, dynamicAdvertisedPort, err := p.getDynamicAdvertisedAddress(cfg.BrokerID, port)
	if err != nil {
		return "", 0, err
	}
	cfg.AdvertisedAddress = net.JoinHostPort(dynamicAdvertisedHost, fmt.Sprint(dynamicAdvertisedPort))
	cfg.ListenerAddress = address

	p.brokerToListenerConfig[brokerAddress] = cfg
	logrus.Infof("Dynamic listener %s for broker %s brokerId %d advertised as %s", cfg.ListenerAddress, cfg.GetBrokerAddress(), cfg.BrokerID, cfg.AdvertisedAddress)

	return dynamicAdvertisedHost, int32(dynamicAdvertisedPort), nil
}

func (p *Listeners) getDynamicAdvertisedAddress(brokerID int32, port int) (string, int, error) {
	dynamicAdvertisedListener := p.dynamicAdvertisedListener
	if dynamicAdvertisedListener == "" {
		return p.defaultListenerIP, port, nil
	}
	dynamicAdvertisedListener, err := p.templateDynamicAdvertisedAddress(brokerID)
	if err != nil {
		return "", 0, err
	}
	var (
		dynamicAdvertisedHost = dynamicAdvertisedListener
		dynamicAdvertisedPort = port
	)
	advHost, advPortStr, err := net.SplitHostPort(dynamicAdvertisedListener)
	if err == nil {
		if advPort, err := strconv.Atoi(advPortStr); err == nil {
			dynamicAdvertisedHost = advHost
			dynamicAdvertisedPort = advPort
		}
	}
	return dynamicAdvertisedHost, dynamicAdvertisedPort, nil
}

func (p *Listeners) templateDynamicAdvertisedAddress(brokerID int32) (string, error) {
	tmpl, err := template.New("dynamicAdvertisedHost").Option("missingkey=error").Parse(p.dynamicAdvertisedListener)
	if err != nil {
		return "", fmt.Errorf("failed to parse host template '%s': %w", p.dynamicAdvertisedListener, err)
	}
	var buf bytes.Buffer
	data := map[string]any{
		"brokerId": brokerID,
		"brokerID": brokerID,
	}
	err = tmpl.Execute(&buf, data)
	if err != nil {
		return "", fmt.Errorf("failed to execute host template '%s': %w", p.dynamicAdvertisedListener, err)
	}
	return buf.String(), nil
}

func (p *Listeners) ListenInstances(cfgs []config.ListenerConfig) (<-chan Conn, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// allows multiple local addresses to point to the remote
	for _, v := range cfgs {
		cfg := FromListenerConfig(v)
		_, err := listenInstance(p.connSrc, cfg, p.tcpConnOptions, p.listenFunc)
		if err != nil {
			return nil, err
		}
	}
	return p.connSrc, nil
}

func listenInstance(dst chan<- Conn, cfg *ListenerConfig, opts TCPConnOptions, listenFunc ListenFunc) (net.Listener, error) {
	l, err := listenFunc(cfg)
	if err != nil {
		return nil, err
	}
	go withRecover(func() {
		for {
			c, err := l.Accept()
			if err != nil {
				logrus.Infof("Error in accept for %q on %v: %v", cfg.ToListenerConfig(), cfg.ListenerAddress, err)
				l.Close()
				return
			}
			if tcpConn, ok := c.(*net.TCPConn); ok {
				if err := opts.setTCPConnOptions(tcpConn); err != nil {
					logrus.Infof("WARNING: Error while setting TCP options for accepted connection %q on %v: %v", cfg.ToListenerConfig(), l.Addr().String(), err)
				}
			}
			brokerAddress := cfg.GetBrokerAddress()
			if cfg.BrokerID != UnknownBrokerID {
				logrus.Infof("New connection for %s brokerId %d", brokerAddress, cfg.BrokerID)
			} else {
				logrus.Infof("New connection for %s", brokerAddress)
			}
			dst <- Conn{BrokerAddress: brokerAddress, LocalConnection: c}
		}
	})
	if cfg.BrokerID != UnknownBrokerID {
		logrus.Infof("Listening on %s (%s) for remote %s broker %d", cfg.ListenerAddress, l.Addr().String(), cfg.GetBrokerAddress(), cfg.BrokerID)
	} else {
		logrus.Infof("Listening on %s (%s) for remote %s", cfg.ListenerAddress, l.Addr().String(), cfg.GetBrokerAddress())
	}
	return l, nil
}
