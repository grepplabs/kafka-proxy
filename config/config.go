package config

import (
	"fmt"
	"github.com/grepplabs/kafka-proxy/pkg/libs/util"
	"github.com/pkg/errors"
	"net"
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
		Format string
		Level  string
	}
	Proxy struct {
		DefaultListenerIP       string
		BootstrapServers        []ListenerConfig
		ExternalServers         []ListenerConfig
		DisableDynamicListeners bool
		RequestBufferSize       int
		ResponseBufferSize      int
		ListenerReadBufferSize  int // SO_RCVBUF
		ListenerWriteBufferSize int // SO_SNDBUF
		ListenerKeepAlive       time.Duration

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
			Enable             bool
			InsecureSkipVerify bool
			ClientCertFile     string
			ClientKeyFile      string
			ClientKeyPassword  string
			CAChainCertFile    string
		}

		SASL struct {
			Enable         bool
			Username       string
			Password       string
			JaasConfigFile string
		}
	}
	Socks5 struct {
		ProxyAddress string
		Username     string
		Password     string
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
	if c.Kafka.SASL.Enable && (c.Kafka.SASL.Username == "" || c.Kafka.SASL.Password == "") {
		return errors.New("SASL.Username and SASL.Password are required when SASL is enabled")
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
	if c.Auth.Local.Enable && c.Auth.Local.Command == "" {
		return errors.New("Command is required when Auth.Local.Enable is enabled")
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
	if c.Socks5.ProxyAddress == "" && (c.Socks5.Username != "" || c.Socks5.Password != "") {
		return errors.New("Socks5.ProxyAddress must not be empty when Socks5 Username/Password is provided")
	}
	if (c.Socks5.Username != "" && c.Socks5.Password == "") || (c.Socks5.Username == "" && c.Socks5.Password != "") {
		return errors.New("Both Socks5 Username and Password must be provided provided")
	}
	if len(c.Socks5.Username) > 255 || len(c.Socks5.Password) > 255 {
		// RFC1929
		return errors.New("Max length of Socks5 Username/Password is 255 chars")
	}
	if c.Socks5.ProxyAddress != "" {
		if _, _, err := util.SplitHostPort(c.Socks5.ProxyAddress); err != nil {
			return err
		}
	}

	return nil
}
