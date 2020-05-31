package config

import (
	"fmt"
	"github.com/grepplabs/kafka-proxy/pkg/libs/util"
	"github.com/pkg/errors"
	"net"
	"net/url"
	"strings"
	"time"
)

const defaultClientID = "kafka-proxy"

var (
	// Version is the current version of the app, generated at build time
	Version = "unknown"
)

type NetAddressMappingFunc func(brokerHost string, brokerPort int32) (listenerHost string, listenerPort int32, err error)

type ListenerConfig struct {
	BrokerAddress     string
	ListenerAddress   string
	AdvertisedAddress string
}
type DialAddressMapping struct {
	SourceAddress      string
	DestinationAddress string
}

type Config struct {
	Http struct {
		ListenAddress string
		MetricsPath   string
		HealthPath    string
		Disable       bool
	}
	Debug struct {
		ListenAddress string
		DebugPath     string
		Enabled       bool
	}
	Log struct {
		Format         string
		Level          string
		LevelFieldName string
		TimeFiledName  string
		MsgFiledName   string
	}
	Proxy struct {
		DefaultListenerIP        string
		BootstrapServers         []ListenerConfig
		ExternalServers          []ListenerConfig
		DialAddressMappings      []DialAddressMapping
		DisableDynamicListeners  bool
		DynamicSequentialMinPort int
		RequestBufferSize        int
		ResponseBufferSize       int
		ListenerReadBufferSize   int // SO_RCVBUF
		ListenerWriteBufferSize  int // SO_SNDBUF
		ListenerKeepAlive        time.Duration

		TLS struct {
			Enable                   bool
			ListenerCertFile         string
			ListenerKeyFile          string
			ListenerKeyPassword      string
			CAChainCertFile          string
			ListenerCipherSuites     []string
			ListenerCurvePreferences []string
		}
	}
	Auth struct {
		Local struct {
			Enable     bool
			Command    string
			Mechanism  string
			Parameters []string
			LogLevel   string
			Timeout    time.Duration
		}
		Gateway struct {
			Client struct {
				Enable     bool
				Method     string
				Magic      uint64
				Command    string
				Parameters []string
				LogLevel   string
				Timeout    time.Duration
			}
			Server struct {
				Enable     bool
				Method     string
				Magic      uint64
				Command    string
				Parameters []string
				LogLevel   string
				Timeout    time.Duration
			}
		}
	}
	Kafka struct {
		ClientID string

		MaxOpenRequests int

		ForbiddenApiKeys []int

		DialTimeout               time.Duration // How long to wait for the initial connection.
		WriteTimeout              time.Duration // How long to wait for a request.
		ReadTimeout               time.Duration // How long to wait for a response.
		KeepAlive                 time.Duration
		ConnectionReadBufferSize  int // SO_RCVBUF
		ConnectionWriteBufferSize int // SO_SNDBUF

		TLS struct {
			Enable               bool
			InsecureSkipVerify   bool
			ClientCertFile       string
			ClientKeyFile        string
			ClientKeyPassword    string
			CAChainCertFile      string
			SameClientCertEnable bool
		}

		SASL struct {
			Enable         bool
			Username       string
			Password       string
			JaasConfigFile string
			Method         string
			Plugin         struct {
				Enable     bool
				Command    string
				Mechanism  string
				Parameters []string
				LogLevel   string
				Timeout    time.Duration
			}
		}
	}
	ForwardProxy struct {
		Url string

		Scheme   string
		Address  string
		Username string
		Password string
	}
}

func (c *Config) InitBootstrapServers(bootstrapServersMapping []string) (err error) {
	c.Proxy.BootstrapServers, err = getListenerConfigs(bootstrapServersMapping)
	return err
}

func (c *Config) InitExternalServers(externalServersMapping []string) (err error) {
	c.Proxy.ExternalServers, err = getListenerConfigs(externalServersMapping)
	return err
}

func (c *Config) InitDialAddressMappings(dialMappings []string) (err error) {
	c.Proxy.DialAddressMappings, err = getDialAddressMappings(dialMappings)
	return err
}

func (c *Config) InitSASLCredentials() (err error) {
	if c.Kafka.SASL.JaasConfigFile != "" {
		credentials, err := NewJaasCredentialFromFile(c.Kafka.SASL.JaasConfigFile)
		if err != nil {
			return err
		}
		c.Kafka.SASL.Username = credentials.Username
		c.Kafka.SASL.Password = credentials.Password
	}
	return nil
}
func getDialAddressMappings(dialMapping []string) ([]DialAddressMapping, error) {
	dialMappings := make([]DialAddressMapping, 0)
	if dialMapping != nil {
		for _, v := range dialMapping {
			pair := strings.Split(v, ",")
			if len(pair) != 2 {
				return nil, errors.New("dial-mapping must be in form 'srchost:srcport,dsthost:dstport'")
			}
			srcHost, srcPort, err := util.SplitHostPort(pair[0])
			if err != nil {
				return nil, err
			}
			dstHost, dstPort, err := util.SplitHostPort(pair[1])
			if err != nil {
				return nil, err
			}
			dialMapping := DialAddressMapping{
				SourceAddress:      net.JoinHostPort(srcHost, fmt.Sprint(srcPort)),
				DestinationAddress: net.JoinHostPort(dstHost, fmt.Sprint(dstPort))}
			dialMappings = append(dialMappings, dialMapping)
		}
	}
	return dialMappings, nil
}

func getListenerConfigs(serversMapping []string) ([]ListenerConfig, error) {
	listenerConfigs := make([]ListenerConfig, 0)
	if serversMapping != nil {
		for _, v := range serversMapping {
			pair := strings.Split(v, ",")
			if len(pair) != 2 && len(pair) != 3 {
				return nil, errors.New("server-mapping must be in form 'remotehost:remoteport,localhost:localport(,advhost:advport)'")
			}
			remoteHost, remotePort, err := util.SplitHostPort(pair[0])
			if err != nil {
				return nil, err
			}
			localHost, localPort, err := util.SplitHostPort(pair[1])
			if err != nil {
				return nil, err
			}
			advertisedHost, advertisedPort := localHost, localPort
			if len(pair) == 3 {
				advertisedHost, advertisedPort, err = util.SplitHostPort(pair[2])
				if err != nil {
					return nil, err
				}
			}

			listenerConfig := ListenerConfig{
				BrokerAddress:     net.JoinHostPort(remoteHost, fmt.Sprint(remotePort)),
				ListenerAddress:   net.JoinHostPort(localHost, fmt.Sprint(localPort)),
				AdvertisedAddress: net.JoinHostPort(advertisedHost, fmt.Sprint(advertisedPort))}
			listenerConfigs = append(listenerConfigs, listenerConfig)
		}
	}
	return listenerConfigs, nil
}

func NewConfig() *Config {
	c := &Config{}

	c.Kafka.ClientID = defaultClientID
	c.Kafka.MaxOpenRequests = 256
	c.Kafka.DialTimeout = 15 * time.Second
	c.Kafka.ReadTimeout = 30 * time.Second
	c.Kafka.WriteTimeout = 30 * time.Second
	c.Kafka.KeepAlive = 60 * time.Second
	c.Kafka.ForbiddenApiKeys = make([]int, 0)

	c.Http.MetricsPath = "/metrics"
	c.Http.HealthPath = "/health"

	c.Proxy.DefaultListenerIP = "127.0.0.1"
	c.Proxy.DisableDynamicListeners = false
	c.Proxy.RequestBufferSize = 4096
	c.Proxy.ResponseBufferSize = 4096
	c.Proxy.ListenerKeepAlive = 60 * time.Second

	return c
}

func (c *Config) Validate() error {
	if c.Kafka.SASL.Enable {
		if c.Kafka.SASL.Plugin.Enable {
			if c.Kafka.SASL.Plugin.Command == "" {
				return errors.New("Command is required when Kafka.SASL.Plugin.Enable is enabled")
			}
			if c.Kafka.SASL.Plugin.Timeout <= 0 {
				return errors.New("Kafka.SASL.Plugin.Timeout must be greater than 0")
			}
			if c.Kafka.SASL.Plugin.Mechanism != "OAUTHBEARER" {
				return errors.New("Mechanism OAUTHBEARER is required when Kafka.SASL.Plugin.Enable is enabled")
			}
		} else {
			if c.Kafka.SASL.Username == "" || c.Kafka.SASL.Password == "" {
				return errors.New("SASL.Username and SASL.Password are required when SASL is enabled and plugin is not used")
			}
		}
	} else {
		if c.Kafka.SASL.Plugin.Enable {
			return errors.New("Kafka.SASL.Plugin.Enable must be disabled, when SASL is disabled")
		}
	}
	if c.Kafka.KeepAlive < 0 {
		return errors.New("KeepAlive must be greater or equal 0")
	}
	if c.Kafka.DialTimeout < 0 {
		return errors.New("DialTimeout must be greater or equal 0")
	}
	if c.Kafka.ReadTimeout < 0 {
		return errors.New("ReadTimeout must be greater or equal 0")
	}
	if c.Kafka.WriteTimeout < 0 {
		return errors.New("WriteTimeout must be greater or equal 0")
	}

	if c.Kafka.MaxOpenRequests < 1 {
		return errors.New("MaxOpenRequests must be greater than 0")
	}
	// proxy
	if c.Proxy.BootstrapServers == nil || len(c.Proxy.BootstrapServers) == 0 {
		return errors.New("list of bootstrap-server-mapping must not be empty")
	}
	if c.Proxy.DefaultListenerIP == "" {
		return errors.New("DefaultListenerIP must not be empty")
	}
	if net.ParseIP(c.Proxy.DefaultListenerIP) == nil {
		return errors.New("DefaultListerIP is not a valid IP")
	}
	if c.Proxy.RequestBufferSize < 1 {
		return errors.New("RequestBufferSize must be greater than 0")
	}
	if c.Proxy.ResponseBufferSize < 1 {
		return errors.New("ResponseBufferSize must be greater than 0")
	}
	if c.Proxy.ListenerKeepAlive < 0 {
		return errors.New("ListenerKeepAlive must be greater or equal 0")
	}
	if c.Proxy.TLS.Enable && (c.Proxy.TLS.ListenerKeyFile == "" || c.Proxy.TLS.ListenerCertFile == "") {
		return errors.New("ListenerKeyFile and ListenerCertFile are required when Proxy TLS is enabled")
	}
	if c.Kafka.TLS.SameClientCertEnable && (!c.Kafka.TLS.Enable || c.Kafka.TLS.ClientCertFile == "" || !c.Proxy.TLS.Enable) {
		return errors.New("ClientCertFile is required on Kafka TLS and TLS must be enabled on both Proxy and Kafka connections when SameClientCertEnable is enabled")
	}
	if c.Auth.Local.Enable && c.Auth.Local.Command == "" {
		return errors.New("Command is required when Auth.Local.Enable is enabled")
	}
	if c.Auth.Local.Enable && (c.Auth.Local.Mechanism != "PLAIN" && c.Auth.Local.Mechanism != "OAUTHBEARER") {
		return errors.New("Mechanism PLAIN or OAUTHBEARER is required when Auth.Local.Enable is enabled")
	}
	if c.Auth.Local.Enable && c.Auth.Local.Timeout <= 0 {
		return errors.New("Auth.Local.Timeout must be greater than 0")
	}
	if c.Auth.Gateway.Client.Enable && (c.Auth.Gateway.Client.Command == "" || c.Auth.Gateway.Client.Method == "" || c.Auth.Gateway.Client.Magic == 0) {
		return errors.New("Command, Method and Magic are required when Auth.Gateway.Client.Enable is enabled")
	}
	if c.Auth.Gateway.Client.Enable && c.Auth.Gateway.Client.Timeout <= 0 {
		return errors.New("Auth.Gateway.Client.Timeout must be greater than 0")
	}

	if c.Auth.Gateway.Server.Enable && (c.Auth.Gateway.Server.Command == "" || c.Auth.Gateway.Server.Method == "" || c.Auth.Gateway.Server.Magic == 0) {
		return errors.New("Command, Method and Magic are required when Auth.Gateway.Server.Enable is enabled")
	}
	if c.Auth.Gateway.Server.Enable && c.Auth.Gateway.Server.Timeout <= 0 {
		return errors.New("Auth.Gateway.Server.Timeout must be greater than 0")
	}
	// http://username:password@hostname:port or socks5://username:password@hostname:port
	if c.ForwardProxy.Url != "" {
		var proxyUrl *url.URL
		var err error
		if proxyUrl, err = url.Parse(c.ForwardProxy.Url); err != nil {
			return err
		}
		if proxyUrl.Port() == "" {
			return errors.New("Port part of ForwardProxy.Url must not be empty")
		}
		c.ForwardProxy.Address = proxyUrl.Host

		if proxyUrl.Scheme != "http" && proxyUrl.Scheme != "socks5" {
			return errors.New("ForwardProxy.Url Scheme must be http or socks5")
		}
		c.ForwardProxy.Scheme = proxyUrl.Scheme

		if proxyUrl.User != nil {
			password, _ := proxyUrl.User.Password()
			if proxyUrl.User.Username() == "" || password == "" {
				return errors.New("Both ForwardProxy Url Username and Password must be provided")
			}
			c.ForwardProxy.Username = proxyUrl.User.Username()
			c.ForwardProxy.Password = password
		}

	}
	return nil
}
