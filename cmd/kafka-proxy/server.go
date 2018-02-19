package server

import (
	"fmt"
	"github.com/grepplabs/kafka-proxy/config"
	"github.com/grepplabs/kafka-proxy/proxy"
	"github.com/oklog/oklog/pkg/group"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
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
	Run: Run,
}

func init() {
	// proxy
	Server.Flags().StringVar(&c.Proxy.DefaultListenerIP, "default-listener-ip", "127.0.0.1", "Default listener IP")
	Server.Flags().StringArrayVar(&bootstrapServersMapping, "bootstrap-server-mapping", []string{}, "Mapping of Kafka bootstrap server address to local address (host:port,host:port)")
	Server.MarkFlagRequired("bootstrap-server-mapping")
	Server.Flags().IntVar(&c.Proxy.RequestBufferSize, "proxy-request-buffer-size", 4096, "Request buffer size pro tcp connection")
	Server.Flags().IntVar(&c.Proxy.ResponseBufferSize, "proxy-response-buffer-size", 4096, "Response buffer size pro tcp connection")

	Server.Flags().IntVar(&c.Proxy.ListenerReadBufferSize, "proxy-listener-read-buffer-size", 0, "Size of the operating system's receive buffer associated with the connection. If zero, system default is used")
	Server.Flags().IntVar(&c.Proxy.ListenerWriteBufferSize, "proxy-listener-write-buffer-size", 0, "Sets the size of the operating system's transmit buffer associated with the connection. If zero, system default is used")
	Server.Flags().DurationVar(&c.Proxy.ListenerKeepAlive, "proxy-listener-keep-alive", 60*time.Second, "Keep alive period for an active network connection. If zero, keep-alives are disabled")

	// kafka
	Server.Flags().StringVar(&c.Kafka.ClientID, "kafka-client-id", "kafka-proxy", "An optional identifier to track the source of requests")
	Server.Flags().IntVar(&c.Kafka.MaxOpenRequests, "kafka-max-open-requests", 256, "Maximal number of open requests pro tcp connection before sending on it blocks")
	Server.Flags().DurationVar(&c.Kafka.DialTimeout, "kafka-dial-timeout", 15*time.Second, "How long to wait for the initial connection")
	Server.Flags().DurationVar(&c.Kafka.WriteTimeout, "kafka-write-timeout", 30*time.Second, "How long to wait for a transmit")
	Server.Flags().DurationVar(&c.Kafka.ReadTimeout, "kafka-read-timeout", 30*time.Second, "How long to wait for a response")
	Server.Flags().DurationVar(&c.Kafka.KeepAlive, "kafka-keep-alive", 60*time.Second, "Keep alive period for an active network connection. If zero, keep-alives are disabled")
	Server.Flags().IntVar(&c.Kafka.ConnectionReadBufferSize, "kafka-connection-read-buffer-size", 0, "Size of the operating system's receive buffer associated with the connection. If zero, system default is used")
	Server.Flags().IntVar(&c.Kafka.ConnectionWriteBufferSize, "kafka-connection-write-buffer-size", 0, "Sets the size of the operating system's transmit buffer associated with the connection. If zero, system default is used")

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
	Server.Flags().BoolVar(&c.Http.Disable, "http-disable", false, "Disable HTTP endpoints")
	Server.Flags().StringVar(&c.Http.ListenAddress, "http-listen-address", "0.0.0.0:9080", "Address that kafka-proxy is listening on")
	Server.Flags().StringVar(&c.Http.MetricsPath, "http-metrics-path", "/metrics", "Path on which to expose metrics")
	Server.Flags().StringVar(&c.Http.HealthPath, "http-health-path", "/health", "Path on which to health endpoint")

	// Debug
	Server.Flags().BoolVar(&c.Debug.Enabled, "debug-enable", false, "Enable Debug endpoint")
	Server.Flags().StringVar(&c.Debug.ListenAddress, "debug-listen-address", "0.0.0.0:6060", "Debug listen address")

}

func Run(_ *cobra.Command, _ []string) {
	log.Printf("Starting kafka-proxy version %s", config.Version)

	var g group.Group
	{
		// All active connections are stored in this variable.
		connset := proxy.NewConnSet()
		prometheus.MustRegister(proxy.NewCollector(connset))
		listenerTCPConnOptions := proxy.TCPConnOptions{
			KeepAlive:       c.Proxy.ListenerKeepAlive,
			ReadBufferSize:  c.Proxy.ListenerReadBufferSize,
			WriteBufferSize: c.Proxy.ListenerWriteBufferSize,
		}
		listeners := proxy.NewListeners(c.Proxy.DefaultListenerIP, listenerTCPConnOptions)
		connSrc, err := listeners.ListenInstances(c.Proxy.BootstrapServers)
		if err != nil {
			log.Fatal(err)
		}

		proxyClient, err := proxy.NewClient(connset, c, listeners.GetNetAddressMapping)
		if err != nil {
			log.Fatal(err)
		}
		g.Add(func() error {
			log.Print("Ready for new connections")
			return proxyClient.Run(connSrc)
		}, func(error) {
			proxyClient.Close()
		})
	}
	{
		cancelInterrupt := make(chan struct{})
		g.Add(func() error {
			c := make(chan os.Signal, 1)
			signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
			select {
			case sig := <-c:
				return fmt.Errorf("received signal %s", sig)
			case <-cancelInterrupt:
				return nil
			}
		}, func(error) {
			close(cancelInterrupt)
		})
	}
	if !c.Http.Disable {
		httpListener, err := net.Listen("tcp", c.Http.ListenAddress)
		if err != nil {
			log.Fatal(err)
		}
		g.Add(func() error {
			return http.Serve(httpListener, NewHTTPHandler())
		}, func(error) {
			httpListener.Close()
		})
	}
	if c.Debug.Enabled {
		// https://golang.org/pkg/net/http/pprof/
		// https://jvns.ca/blog/2017/09/24/profiling-go-with-pprof/
		debugListener, err := net.Listen("tcp", c.Debug.ListenAddress)
		if err != nil {
			log.Fatal(err)
		}
		g.Add(func() error {
			return http.Serve(debugListener, http.DefaultServeMux)
		}, func(error) {
			debugListener.Close()
		})
	}
	err := g.Run()
	log.Println("Exit", err)
}

func NewHTTPHandler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(
			`<html>
				<head>
					<title>kafka-proxy service</title>
				</head>
				<body>
					<h1>Kafka Proxy</h1>
					<p><a href='` + c.Http.MetricsPath + `'>Metrics</a></p>
				</body>
	        </html>`))
	})
	m.HandleFunc(c.Http.HealthPath, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`OK`))
	})
	m.Handle(c.Http.MetricsPath, promhttp.Handler())

	return m
}
