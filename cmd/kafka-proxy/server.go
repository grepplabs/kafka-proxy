package server

import (
	"github.com/grepplabs/kafka-proxy/config"
	"github.com/grepplabs/kafka-proxy/proxy"
	"github.com/spf13/cobra"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	c = new(config.Config)

	bootstrapServersMapping = make([]string, 0)
)

var Server = &cobra.Command{
	Use:   "server",
	Short: "Run the kafka-proxy server",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if err := c.InitSASLCredentials(); err != nil {
			return err
		}
		if err := c.InitBootstrapServers(bootstrapServersMapping); err != nil {
			return err
		}
		if err := c.Validate(); err != nil {
			return err
		}
		return nil
	},
	RunE: RunE,
}

func init() {
	// proxy
	Server.Flags().StringVar(&c.Proxy.DefaultListenerIP, "default-listener-ip", "127.0.0.1", "Default listener IP")
	Server.Flags().StringArrayVar(&bootstrapServersMapping, "bootstrap-server-mapping", []string{}, "Mapping of Kafka bootstrap server address to local address (host:port,host:port)")
	Server.MarkFlagRequired("bootstrap-server-mapping")

	// kafka
	Server.Flags().StringVar(&c.Kafka.ClientID, "kafka-client-id", "kafka-proxy", "An optional identifier to track the source of requests")
	Server.Flags().IntVar(&c.Kafka.MaxOpenRequests, "kafka-max-open-requests", 256, "Maximal number of open requests pro tcp connection before sending on it blocks")
	Server.Flags().DurationVar(&c.Kafka.DialTimeout, "kafka-dial-timeout", 30*time.Second, "How long to wait for the initial connection")
	Server.Flags().DurationVar(&c.Kafka.WriteTimeout, "kafka-write-timeout", 30*time.Second, "How long to wait for a transmit")
	Server.Flags().DurationVar(&c.Kafka.ReadTimeout, "kafka-read-timeout", 30*time.Second, "How long to wait for a response")
	Server.Flags().DurationVar(&c.Kafka.KeepAlive, "kafka-keep-alive", 60*time.Second, "Keep alive period for an active network connection. If zero, keep-alives are disabled")

	// TLS
	Server.Flags().BoolVar(&c.Kafka.TLS.Enable, "tls-enable", false, "Whether or not to use TLS when connecting to the broker")
	Server.Flags().BoolVar(&c.Kafka.TLS.InsecureSkipVerify, "tls-insecure-skip-verify", false, "It controls whether a client verifies the server's certificate chain and host name")
	Server.Flags().StringVar(&c.Kafka.TLS.ClientCertFile, "tls-client-cert-file", "", "PEM encoded file with client certificate")
	Server.Flags().StringVar(&c.Kafka.TLS.ClientKeyFile, "tls-client-key-file", "", "PEM encoded file with private key for the client certificate")
	Server.Flags().StringVar(&c.Kafka.TLS.ClientKeyPassword, "tls-client-key-password", "", "Password to decrypt rsa private key")
	Server.Flags().StringVar(&c.Kafka.TLS.CAChainCertFile, "tls-ca-chain-cert-file", "", "PEM encoded CA's certificate file")

	// SASL
	Server.Flags().BoolVar(&c.Kafka.SASL.Enable, "sasl-enable", false, "Connect using SASL/PLAIN")
	Server.Flags().StringVar(&c.Kafka.SASL.Username, "sasl-username", "", "SASL user name")
	Server.Flags().StringVar(&c.Kafka.SASL.Password, "sasl-password", "", "SASL user password")
	Server.Flags().StringVar(&c.Kafka.SASL.JaasConfigFile, "sasl-jaas-config-file", "", "Location of JAAS config file with SASL username and password")

	// Web
	Server.Flags().BoolVar(&c.Web.Disable, "web-disable", false, "Disable HTTP endpoints")
	Server.Flags().StringVar(&c.Web.ListenAddress, "web-listen-address", "0.0.0.0:9080", "Address that kafka-proxy is listening on")
	Server.Flags().StringVar(&c.Web.MetricsPath, "web-metrics-path", "/metrics", "Path on which to expose metrics")
	Server.Flags().StringVar(&c.Web.HealthPath, "web-health-path", "/health", "Path on which to health endpoint")
}

func RunE(_ *cobra.Command, _ []string) error {
	log.Printf("Starting kafka-proxy version %s", config.Version)

	// All active connections are stored in this variable.
	connset := proxy.NewConnSet()

	listeners := proxy.NewListeners(c.Proxy.DefaultListenerIP)
	connSrc, err := listeners.ListenInstances(c.Proxy.BootstrapServers)
	if err != nil {
		return err
	}

	proxyClient, err := proxy.NewClient(connset, c, listeners.GetNetAddressMapping)
	if err != nil {
		return err
	}

	log.Print("Ready for new connections")

	stopChan := make(chan struct{}, 1)
	go handleStop(stopChan)

	proxyClient.Run(stopChan, connSrc)

	return nil
}

func handleStop(stopChan chan struct{}) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	<-signals
	log.Print("Received stop signal ...")
	close(stopChan)
}
