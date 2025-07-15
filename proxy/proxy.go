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

type ListenFunc func(listenAddress string) (l net.Listener, err error)

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
	tlsConfig  *tls.Config

	deterministicListeners    bool
	disableDynamicListeners   bool
	dynamicSequentialMinPort  uint16
	currentDynamicPortCounter uint64
	dynamicSequentialMaxPorts uint16

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

	listenFunc := func(listenAddress string) (ln net.Listener, err error) {
		if tlsConfig != nil {
			logrus.Infof("%s: Starting tls listener", listenAddress)
			return tls.Listen("tcp", listenAddress, tlsConfig)
		}
		return net.Listen("tcp", listenAddress)
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
		tlsConfig:                 tlsConfig,
		tcpConnOptions:            tcpConnOptions,
		listenFunc:                listenFunc,
		deterministicListeners:    cfg.Proxy.DeterministicListeners,
		disableDynamicListeners:   cfg.Proxy.DisableDynamicListeners,
		dynamicSequentialMinPort:  cfg.Proxy.DynamicSequentialMinPort,
		currentDynamicPortCounter: 0,
		dynamicSequentialMaxPorts: cfg.Proxy.DynamicSequentialMaxPorts,
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

func (p *Listeners) GetNetAddressMapping(brokerHost string, brokerPort int32, brokerId int32) (advertisedHost string, advertisedPort int32, err error) {
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
	if len(p.dynamicAdvertisedListener) == 0 {
		return "", 0, fmt.Errorf("net address mapping for %s:%d was not found", brokerHost, brokerPort)
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	dynamicAdvertisedHost, dynamicAdvertisedPort, err := p.getDynamicAdvertisedAddress(brokerId, int(brokerPort))
	if err != nil {
		return "", 0, err
	}
	// TODO: what should be the p.defaultListenerIP for the listener?
	cfg := NewListenerConfig(
		brokerAddress,
		net.JoinHostPort(p.defaultListenerIP, fmt.Sprintf("%v", dynamicAdvertisedPort)),
		net.JoinHostPort(dynamicAdvertisedHost, fmt.Sprint(dynamicAdvertisedPort)),
		brokerId,
	)
	p.brokerToListenerConfig[brokerAddress] = cfg
	return util.SplitHostPort(cfg.GetAdvertisedAddress())
}

func (p *Listeners) findListenerConfig(brokerId int32) *ListenerConfig {
	for _, listenerConfig := range p.brokerToListenerConfig {
		if listenerConfig.BrokerID == brokerId {
			return listenerConfig
		}
	}
	return nil
}

// Make sure all dynamically allocated ports are within the half open interval
// [dynamicSequentialMinPort, dynamicSequentialMinPort + dynamicSequentialMaxPorts).
func (p *Listeners) nextDynamicPort(portOffset uint64, brokerAddress string, brokerId int32) (uint16, error) {
	if p.dynamicSequentialMaxPorts == 0 {
		return 0, fmt.Errorf("dynamic sequential max ports is 0")
	}
	port := p.dynamicSequentialMinPort + uint16(portOffset%uint64(p.dynamicSequentialMaxPorts))
	if port < p.dynamicSequentialMinPort {
		return 0, fmt.Errorf("port assignment overflow %s %d: %d", brokerAddress, brokerId, port)
	}
	return port, nil
}

// ListenDynamicInstance creates a new listener for the upstream broker address and broker id.
func (p *Listeners) ListenDynamicInstance(brokerAddress string, brokerId int32) (string, int32, error) {

	fmt.Println("proxy/proxy.go: ListenDynamicInstance:", "brokerAddress", brokerAddress, "brokerId", brokerId)
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
		deterministicPort, err := p.nextDynamicPort(uint64(brokerId), brokerAddress, brokerId)
		if err != nil {
			return "", 0, err
		}
		listenerAddress = net.JoinHostPort(p.defaultListenerIP, strconv.Itoa(int(deterministicPort)))
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
		if p.dynamicSequentialMinPort == 0 {
			// Use random (non sequential) ephemeral free port, allocated by OS.
			listenerAddress = net.JoinHostPort(p.defaultListenerIP, strconv.Itoa(0))
		} else {
			// Use sequentially allocated port.
			port, err := p.nextDynamicPort(p.currentDynamicPortCounter, brokerAddress, brokerId)
			if err != nil {
				return "", 0, err
			}
			listenerAddress = net.JoinHostPort(p.defaultListenerIP, strconv.Itoa(int(port)))
			p.currentDynamicPortCounter += 1
		}
	}

	cfg := NewListenerConfig(brokerAddress, listenerAddress, "", brokerId)
	l, err := p.listenInstance(p.connSrc, cfg, p.tcpConnOptions, p.listenFunc, p)
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

func (p *Listeners) GetBrokerAddressByAdvertisedHost(host string) (brokerAddress string, brokerId int32, err error) {

	// TODO: Use a better algorithm than a for loop
	// Maybe use a bi-directional map? I believe downstream host-port always has only one upstream hostport.
	for brokerAddr, c := range p.brokerToListenerConfig {
		if c == nil {
			continue
		}
		advertisedHost, _, err := net.SplitHostPort(c.GetAdvertisedAddress())
		if err != nil {
			fmt.Println("failed to split address", err)
		}
		if advertisedHost == host {
			return brokerAddr, c.GetBrokerID(), nil
		}
	}

	return "", 0, fmt.Errorf("broker not found for advertised host %q", host)
}

func (p *Listeners) ListenInstances(cfgs []config.ListenerConfig) (<-chan Conn, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	for _, v := range cfgs {
		cfg := FromListenerConfig(v)
		_, err := p.listenInstance(p.connSrc, cfg, p.tcpConnOptions, p.listenFunc, p)
		if err != nil {
			return nil, err
		}
	}
	return p.connSrc, nil
}

type BrokerConfigMap interface {
	ToListenerConfig() config.ListenerConfig
	GetListenerAddress() string
	GetBrokerAddress() string
	GetAdvertisedAddress() string
	GetBrokerID() int32
}

type HostBasedRouting interface {
	GetBrokerAddressByAdvertisedHost(host string) (brokerAddress string, brokerId int32, err error)
}

func (p *Listeners) listenInstance(dst chan<- Conn, cfg BrokerConfigMap, opts TCPConnOptions, listenFunc ListenFunc, brokers HostBasedRouting) (net.Listener, error) {

	l, err := listenFunc(cfg.GetListenerAddress())
	if err != nil {
		return nil, err
	}
	go withRecover(func() {
		for {
			c, err := l.Accept()
			if err != nil {
				logrus.Infof("Error in accept for %q on %v: %v", cfg.ToListenerConfig(), l.Addr(), err)
				l.Close()
				return
			}

			logrus.Infof("%s: Client(%s) Accepted connection", l.Addr().String(), c.RemoteAddr().String())

			var (
				clientServerName string
				underlyingConn   net.Conn = c
			)

			if conn, ok := underlyingConn.(*tls.Conn); ok {
				logrus.Infof("%s: Client(%s) Accepted tls connection", l.Addr().String(), c.RemoteAddr().String())

				tlsState := conn.ConnectionState()

				if !tlsState.HandshakeComplete {
					err := conn.Handshake()
					if err != nil {
						logrus.Warnf("downstream client tls handshake error: %s", err)
					}
				}
				tlsState = conn.ConnectionState()

				logrus.Infof("%s: Client(%s) Accepted tls connection with server name: %q", l.Addr().String(), c.RemoteAddr().String(), tlsState.ServerName)
				if tlsState.ServerName == "" {
					logrus.Warnf("%s: Client(%s) tls server name could not be read, defaulting to advertised %s", l.Addr().String(), c.RemoteAddr().String(), cfg.GetAdvertisedAddress())
					advertisedHost, _, _ := net.SplitHostPort(cfg.GetAdvertisedAddress())
					clientServerName = advertisedHost
				} else {
					clientServerName = tlsState.ServerName
					logrus.Infof("%s: Client(%s) Discovered server name from tls client hello: %s", l.Addr().String(), c.RemoteAddr().String(), clientServerName)
				}
				underlyingConn = conn.NetConn()
			}

			if conn, ok := underlyingConn.(*net.TCPConn); ok {
				if err := opts.setTCPConnOptions(conn); err != nil {
					logrus.Infof("WARNING: Error while setting TCP options for accepted connection %q on %v: %v", cfg.ToListenerConfig(), l.Addr().String(), err)
				}
			}

			brokerAddress := cfg.GetBrokerAddress()
			brokerId := cfg.GetBrokerID()

			if clientServerName != "" {
				if address, id, err := brokers.GetBrokerAddressByAdvertisedHost(clientServerName); err == nil {
					brokerAddress = address
					brokerId = id
				} else {
					logrus.Infof("%s: Failed to match host/authority %q with any advertised address", l.Addr(), clientServerName)
				}
			}

			if brokerId == UnknownBrokerID {
				logrus.Infof("%s: Client(%s) connected with server name %q", l.Addr(), c.RemoteAddr().String(), clientServerName)
			} else {
				logrus.Infof("%s: Client(%s) connected for %s brokerId %d with server name %s", l.Addr(), c.RemoteAddr().String(), brokerAddress, brokerId, clientServerName)
			}
			logrus.Infof("%s: Client(%s) proxying starting", l.Addr(), c.RemoteAddr().String())
			dst <- Conn{BrokerAddress: brokerAddress, LocalConnection: c}
		}
	})
	if cfg.GetBrokerID() != UnknownBrokerID {
		logrus.Infof("Listening on %s for remote %s broker %d", l.Addr().String(), cfg.GetBrokerAddress(), cfg.GetBrokerID())
	} else {
		logrus.Infof("Listening on %s for remote %s", l.Addr().String(), cfg.GetBrokerAddress())
	}
	return l, nil
}
