package config

import (
	"github.com/pkg/errors"
	"regexp"
	"time"
)

const defaultClientID = "kafka-proxy"

var (
	// Version is the current version of the app, generated at build time
	Version = "unknown"
)

var validID = regexp.MustCompile(`\A[A-Za-z0-9._-]+\z`)

type ListenerConfig struct {
	BrokerAddress   string
	ListenerAddress string
}

type Config struct {
	Web struct {
		ListenAddress string
		MetricsPath   string
		HealthPath    string
		Disable       bool //Disable http endpoints e.g. for kafkatool
	}
	Proxy struct {
		DefaultListerIP string
		Bootstrap       []ListenerConfig
	}
	Kafka struct {
		ClientID string

		MaxOpenRequests int

		DialTimeout  time.Duration // How long to wait for the initial connection.
		ReadTimeout  time.Duration // How long to wait for a response.
		WriteTimeout time.Duration // How long to wait for a transmit.
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

func NewConfig() *Config {
	c := &Config{}

	c.Kafka.ClientID = defaultClientID
	c.Kafka.MaxOpenRequests = 256
	c.Kafka.DialTimeout = 30 * time.Second
	c.Kafka.ReadTimeout = 30 * time.Second
	c.Kafka.WriteTimeout = 30 * time.Second
	c.Kafka.KeepAlive = 60 * time.Second

	c.Web.MetricsPath = "/metrics"
	c.Web.HealthPath = "/health"

	c.Proxy.DefaultListerIP = "127.0.0.1"

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
		return errors.New("Net.KeepAlive must be greater or equal 0")
	}
	if c.Kafka.DialTimeout < 0 {
		return errors.New("Net.DialTimeout must be greater or equal 0")
	}
	if c.Kafka.ReadTimeout < 0 {
		return errors.New("Net.ReadTimeout must be greater or equal 0")
	}
	if c.Kafka.WriteTimeout < 0 {
		return errors.New("Net.WriteTimeout must be greater or equal 0")
	}

	switch {
	case !validID.MatchString(c.Kafka.ClientID):
		return errors.New("ClientID is invalid")
	}
	return nil
}
