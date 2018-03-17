package server

import (
	"fmt"
	"github.com/grepplabs/kafka-proxy/config"
	"github.com/grepplabs/kafka-proxy/proxy"
	"github.com/oklog/oklog/pkg/group"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"errors"
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	gatewayclient "github.com/grepplabs/kafka-proxy/plugin/gateway-client/shared"
	gatewayserver "github.com/grepplabs/kafka-proxy/plugin/gateway-server/shared"
	localauth "github.com/grepplabs/kafka-proxy/plugin/local-auth/shared"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
)

var (
	c = new(config.Config)

	bootstrapServersMapping = make([]string, 0)
	externalServersMapping  = make([]string, 0)
)

var Server = &cobra.Command{
	Use:   "server",
	Short: "Run the kafka-proxy server",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		SetLogger()

		if err := c.InitSASLCredentials(); err != nil {
			return err
		}
		if err := c.InitBootstrapServers(bootstrapServersMapping); err != nil {
			return err
		}
		if err := c.InitExternalServers(externalServersMapping); err != nil {
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
	Server.Flags().StringArrayVar(&bootstrapServersMapping, "bootstrap-server-mapping", []string{}, "Mapping of Kafka bootstrap server address to local address (host:port,host:port(,advhost:advport))")
	Server.MarkFlagRequired("bootstrap-server-mapping")
	Server.Flags().StringArrayVar(&externalServersMapping, "external-server-mapping", []string{}, "Mapping of Kafka server address to external address (host:port,host:port). A listener for the external address is not started")
	Server.Flags().BoolVar(&c.Proxy.DisableDynamicListeners, "dynamic-listeners-disable", false, "Disable dynamic listeners.")

	Server.Flags().IntVar(&c.Proxy.RequestBufferSize, "proxy-request-buffer-size", 4096, "Request buffer size pro tcp connection")
	Server.Flags().IntVar(&c.Proxy.ResponseBufferSize, "proxy-response-buffer-size", 4096, "Response buffer size pro tcp connection")

	Server.Flags().IntVar(&c.Proxy.ListenerReadBufferSize, "proxy-listener-read-buffer-size", 0, "Size of the operating system's receive buffer associated with the connection. If zero, system default is used")
	Server.Flags().IntVar(&c.Proxy.ListenerWriteBufferSize, "proxy-listener-write-buffer-size", 0, "Sets the size of the operating system's transmit buffer associated with the connection. If zero, system default is used")
	Server.Flags().DurationVar(&c.Proxy.ListenerKeepAlive, "proxy-listener-keep-alive", 60*time.Second, "Keep alive period for an active network connection. If zero, keep-alives are disabled")

	Server.Flags().BoolVar(&c.Proxy.TLS.Enable, "proxy-listener-tls-enable", false, "Whether or not to use TLS listener")
	Server.Flags().StringVar(&c.Proxy.TLS.ListenerCertFile, "proxy-listener-cert-file", "", "PEM encoded file with server certificate")
	Server.Flags().StringVar(&c.Proxy.TLS.ListenerKeyFile, "proxy-listener-key-file", "", "PEM encoded file with private key for the server certificate")
	Server.Flags().StringVar(&c.Proxy.TLS.ListenerKeyPassword, "proxy-listener-key-password", "", "Password to decrypt rsa private key")
	Server.Flags().StringVar(&c.Proxy.TLS.CAChainCertFile, "proxy-listener-ca-chain-cert-file", "", "PEM encoded CA's certificate file")

	// local authentication plugin
	Server.Flags().BoolVar(&c.Auth.Local.Enable, "auth-local-enable", false, "Enable local SASL/PLAIN authentication performed by listener - SASL handshake will not be passed to kafka brokers")
	Server.Flags().StringVar(&c.Auth.Local.Command, "auth-local-command", "", "Path to authentication plugin binary")
	Server.Flags().StringArrayVar(&c.Auth.Local.Parameters, "auth-local-param", []string{}, "Authentication plugin parameter")
	Server.Flags().StringVar(&c.Auth.Local.LogLevel, "auth-local-log-level", "trace", "Log level of the auth plugin")
	Server.Flags().DurationVar(&c.Auth.Local.Timeout, "auth-local-timeout", 10*time.Second, "Authentication timeout")

	Server.Flags().BoolVar(&c.Auth.Gateway.Client.Enable, "auth-gateway-client-enable", false, "Enable gateway client authentication")
	Server.Flags().StringVar(&c.Auth.Gateway.Client.Command, "auth-gateway-client-command", "", "Path to authentication plugin binary")
	Server.Flags().StringArrayVar(&c.Auth.Gateway.Client.Parameters, "auth-gateway-client-param", []string{}, "Authentication plugin parameter")
	Server.Flags().StringVar(&c.Auth.Gateway.Client.LogLevel, "auth-gateway-client-log-level", "trace", "Log level of the auth plugin")
	Server.Flags().StringVar(&c.Auth.Gateway.Client.Method, "auth-gateway-client-method", "", "Authentication method")
	Server.Flags().Uint64Var(&c.Auth.Gateway.Client.Magic, "auth-gateway-client-magic", 0, "Magic bytes sent in the handshake")
	Server.Flags().DurationVar(&c.Auth.Gateway.Client.Timeout, "auth-gateway-client-timeout", 10*time.Second, "Authentication timeout")

	Server.Flags().BoolVar(&c.Auth.Gateway.Server.Enable, "auth-gateway-server-enable", false, "Enable proxy server authentication")
	Server.Flags().StringVar(&c.Auth.Gateway.Server.Command, "auth-gateway-server-command", "", "Path to authentication plugin binary")
	Server.Flags().StringArrayVar(&c.Auth.Gateway.Server.Parameters, "auth-gateway-server-param", []string{}, "Authentication plugin parameter")
	Server.Flags().StringVar(&c.Auth.Gateway.Server.LogLevel, "auth-gateway-server-log-level", "trace", "Log level of the auth plugin")
	Server.Flags().StringVar(&c.Auth.Gateway.Server.Method, "auth-gateway-server-method", "", "Authentication method")
	Server.Flags().Uint64Var(&c.Auth.Gateway.Server.Magic, "auth-gateway-server-magic", 0, "Magic bytes sent in the handshake")
	Server.Flags().DurationVar(&c.Auth.Gateway.Server.Timeout, "auth-gateway-server-timeout", 10*time.Second, "Authentication timeout")

	// kafka
	Server.Flags().StringVar(&c.Kafka.ClientID, "kafka-client-id", "kafka-proxy", "An optional identifier to track the source of requests")
	Server.Flags().IntVar(&c.Kafka.MaxOpenRequests, "kafka-max-open-requests", 256, "Maximal number of open requests pro tcp connection before sending on it blocks")
	Server.Flags().DurationVar(&c.Kafka.DialTimeout, "kafka-dial-timeout", 15*time.Second, "How long to wait for the initial connection")
	Server.Flags().DurationVar(&c.Kafka.WriteTimeout, "kafka-write-timeout", 30*time.Second, "How long to wait for a transmit")
	Server.Flags().DurationVar(&c.Kafka.ReadTimeout, "kafka-read-timeout", 30*time.Second, "How long to wait for a response")
	Server.Flags().DurationVar(&c.Kafka.KeepAlive, "kafka-keep-alive", 60*time.Second, "Keep alive period for an active network connection. If zero, keep-alives are disabled")
	Server.Flags().IntVar(&c.Kafka.ConnectionReadBufferSize, "kafka-connection-read-buffer-size", 0, "Size of the operating system's receive buffer associated with the connection. If zero, system default is used")
	Server.Flags().IntVar(&c.Kafka.ConnectionWriteBufferSize, "kafka-connection-write-buffer-size", 0, "Sets the size of the operating system's transmit buffer associated with the connection. If zero, system default is used")

	// http://kafka.apache.org/protocol.html#protocol_api_keys
	Server.Flags().IntSliceVar(&c.Kafka.ForbiddenApiKeys, "forbidden-api-keys", []int{}, "Forbidden Kafka request types. The restriction should prevent some Kafka operations e.g. 20 - DeleteTopics")

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

	// Logging
	Server.Flags().StringVar(&c.Log.Format, "log-format", "text", "Log format text or json")
	Server.Flags().StringVar(&c.Log.Level, "log-level", "info", "Log level debug, info, warning, error, fatal or panic")
}

func Run(_ *cobra.Command, _ []string) {
	logrus.Infof("Starting kafka-proxy version %s", config.Version)

	var passwordAuthenticator apis.PasswordAuthenticator
	if c.Auth.Local.Enable {
		client := NewLocalAuthPluginClient()
		defer client.Kill()

		rpcClient, err := client.Client()
		if err != nil {
			logrus.Fatal(err)
		}
		raw, err := rpcClient.Dispense("passwordAuthenticator")
		if err != nil {
			logrus.Fatal(err)
		}
		var ok bool
		passwordAuthenticator, ok = raw.(apis.PasswordAuthenticator)
		if !ok {
			logrus.Fatal(errors.New("unsupported PasswordAuthenticator plugin type"))
		}
	}

	var tokenProvider apis.TokenProvider
	if c.Auth.Gateway.Client.Enable {
		client := NewGatewayClientPluginClient()
		defer client.Kill()

		rpcClient, err := client.Client()
		if err != nil {
			logrus.Fatal(err)
		}
		raw, err := rpcClient.Dispense("tokenProvider")
		if err != nil {
			logrus.Fatal(err)
		}
		var ok bool
		tokenProvider, ok = raw.(apis.TokenProvider)
		if !ok {
			logrus.Fatal(errors.New("unsupported TokenProvider plugin type"))
		}
	}

	var tokenInfo apis.TokenInfo
	if c.Auth.Gateway.Server.Enable {
		client := NewGatewayServerPluginClient()
		defer client.Kill()

		rpcClient, err := client.Client()
		if err != nil {
			logrus.Fatal(err)
		}
		raw, err := rpcClient.Dispense("tokenInfo")
		if err != nil {
			logrus.Fatal(err)
		}
		var ok bool
		tokenInfo, ok = raw.(apis.TokenInfo)
		if !ok {
			logrus.Fatal(errors.New("unsupported TokenInfo plugin type"))
		}
	}

	var g group.Group
	{
		// All active connections are stored in this variable.
		connset := proxy.NewConnSet()
		prometheus.MustRegister(proxy.NewCollector(connset))
		listeners, err := proxy.NewListeners(c)
		if err != nil {
			logrus.Fatal(err)
		}
		connSrc, err := listeners.ListenInstances(c.Proxy.BootstrapServers)
		if err != nil {
			logrus.Fatal(err)
		}
		proxyClient, err := proxy.NewClient(connset, c, listeners.GetNetAddressMapping, passwordAuthenticator, tokenProvider, tokenInfo)
		if err != nil {
			logrus.Fatal(err)
		}
		g.Add(func() error {
			logrus.Print("Ready for new connections")
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
			logrus.Fatal(err)
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
			logrus.Fatal(err)
		}
		g.Add(func() error {
			return http.Serve(debugListener, http.DefaultServeMux)
		}, func(error) {
			debugListener.Close()
		})
	}

	err := g.Run()
	logrus.Info("Exit ", err)
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

func SetLogger() {
	if c.Log.Format == "json" {
		formatter := &logrus.JSONFormatter{
			FieldMap: logrus.FieldMap{
				logrus.FieldKeyTime:  "@timestamp",
				logrus.FieldKeyLevel: "@level",
				logrus.FieldKeyMsg:   "@message",
			},
			TimestampFormat: time.RFC3339,
		}
		logrus.SetFormatter(formatter)
	} else {
		logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	}
	level, err := logrus.ParseLevel(c.Log.Level)
	if err != nil {
		logrus.Errorf("Couldn't parse log level: %s", c.Log.Level)
		level = logrus.InfoLevel
	}
	logrus.SetLevel(level)
}

func NewGatewayClientPluginClient() *plugin.Client {
	return NewPluginClient(gatewayclient.Handshake, gatewayclient.PluginMap, c.Auth.Gateway.Client.LogLevel, c.Auth.Gateway.Client.Command, c.Auth.Gateway.Client.Parameters)
}
func NewGatewayServerPluginClient() *plugin.Client {
	return NewPluginClient(gatewayserver.Handshake, gatewayserver.PluginMap, c.Auth.Gateway.Server.LogLevel, c.Auth.Gateway.Server.Command, c.Auth.Gateway.Server.Parameters)
}
func NewLocalAuthPluginClient() *plugin.Client {
	return NewPluginClient(localauth.Handshake, localauth.PluginMap, c.Auth.Local.LogLevel, c.Auth.Local.Command, c.Auth.Local.Parameters)
}

func NewPluginClient(handshakeConfig plugin.HandshakeConfig, plugins map[string]plugin.Plugin, logLevel string, command string, params []string) *plugin.Client {
	jsonFormat := false
	if c.Log.Format == "json" {
		jsonFormat = true
	}
	logger := hclog.New(&hclog.LoggerOptions{
		Output:     os.Stdout,
		Level:      hclog.LevelFromString(logLevel),
		Name:       "plugin",
		JSONFormat: jsonFormat,
		TimeFormat: time.RFC3339,
	})

	return plugin.NewClient(&plugin.ClientConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         plugins,
		Logger:          logger,
		Cmd:             exec.Command(command, params...),
		AllowedProtocols: []plugin.Protocol{
			plugin.ProtocolNetRPC, plugin.ProtocolGRPC},
	})
}
