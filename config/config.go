package config

import (
	"fmt"
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
	BrokerAddress   string
	ListenerAddress string
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
	Proxy struct {
		DefaultListenerIP string
		BootstrapServers  []ListenerConfig
	}
	Kafka struct {
		ClientID string

		MaxOpenRequests int

		DialTimeout  time.Duration // How long to wait for the initial connection.
		WriteTimeout time.Duration // How long to wait for a request.
		ReadTimeout  time.Duration // How long to wait for a response.
		KeepAlive    time.Duration

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
}

func (c *Config) InitBootstrapServers(bootstrapServersMapping []string) (err error) {
	c.Proxy.BootstrapServers, err = getListenerConfigs(bootstrapServersMapping)
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

func getListenerConfigs(bootstrapServersMapping []string) ([]ListenerConfig, error) {
	listenerConfigs := make([]ListenerConfig, 0)
	if bootstrapServersMapping != nil {
		for _, v := range bootstrapServersMapping {
			pair := strings.Split(v, ",")
			if len(pair) != 2 {
				return nil, errors.New("bootstrap-server-mapping must be in form 'remotehost:remoteport,localhost:localport'")
			}
			remotehost, remoteport, err := SplitHostPort(pair[0])
			if err != nil {
				return nil, err
			}
			localhost, localport, err := SplitHostPort(pair[1])
			if err != nil {
				return nil, err
			}
			listenerConfig := ListenerConfig{BrokerAddress: net.JoinHostPort(remotehost, fmt.Sprint(remoteport)), ListenerAddress: net.JoinHostPort(localhost, fmt.Sprint(localport))}
			listenerConfigs = append(listenerConfigs, listenerConfig)
		}
	}
	return listenerConfigs, nil
}

func NewConfig() *Config {
	c := &Config{}

	c.Kafka.ClientID = defaultClientID
	c.Kafka.MaxOpenRequests = 256
	c.Kafka.DialTimeout = 30 * time.Second
	c.Kafka.ReadTimeout = 30 * time.Second
	c.Kafka.WriteTimeout = 30 * time.Second
	c.Kafka.KeepAlive = 60 * time.Second

	c.Http.MetricsPath = "/metrics"
	c.Http.HealthPath = "/health"

	c.Proxy.DefaultListenerIP = "127.0.0.1"

	return c
}

func (c *Config) Validate() error {
	if c.Kafka.SASL.Enable && (c.Kafka.SASL.Username == "" || c.Kafka.SASL.Password == "") {
		return errors.New("SASL.Username and SASL.Password are required when SASL is enabled")
	}

	if c.Kafka.SASL.Enable && !c.Kafka.TLS.Enable {
		return errors.New("TLS.Enable is required when SASL is enabled")
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
	return nil
}
