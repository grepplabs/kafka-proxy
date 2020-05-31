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
	localauth "github.com/grepplabs/kafka-proxy/plugin/local-auth/shared"
	tokeninfo "github.com/grepplabs/kafka-proxy/plugin/token-info/shared"
	tokenprovider "github.com/grepplabs/kafka-proxy/plugin/token-provider/shared"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"strings"

	"github.com/grepplabs/kafka-proxy/pkg/registry"
	// built-in plugins
	_ "github.com/grepplabs/kafka-proxy/pkg/libs/googleid-info"
	_ "github.com/grepplabs/kafka-proxy/pkg/libs/googleid-provider"
	"github.com/spf13/viper"
)

var (
	c = new(config.Config)

	bootstrapServersMapping = make([]string, 0)
	externalServersMapping  = make([]string, 0)
	dialAddressMapping      = make([]string, 0)
)

var Server = &cobra.Command{
	Use:   "server",
	Short: "Run the kafka-proxy server",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		SetLogger()

		if err := c.InitSASLCredentials(); err != nil {
			return err
		}
		if err := c.InitBootstrapServers(getOrEnvStringSlice(bootstrapServersMapping, "BOOTSTRAP_SERVER_MAPPING")); err != nil {
			return err
		}
		if err := c.InitExternalServers(getOrEnvStringSlice(externalServersMapping, "EXTERNAL_SERVER_MAPPING")); err != nil {
			return err
		}
		if err := c.InitDialAddressMappings(getOrEnvStringSlice(dialAddressMapping, "DIAL_ADDRESS_MAPPING")); err != nil {
			return err
		}
		if err := c.Validate(); err != nil {
			return err
		}
		return nil
	},
	Run: Run,
}

func getOrEnvStringSlice(value []string, envKey string) []string {
	if len(bootstrapServersMapping) != 0 {
		return value
	}
	return strings.Fields(os.Getenv(envKey))
}

func init() {
	initFlags()
}

func initFlags() {
	// proxy
	Server.Flags().StringVar(&c.Proxy.DefaultListenerIP, "default-listener-ip", "127.0.0.1", "Default listener IP")
	Server.Flags().StringArrayVar(&bootstrapServersMapping, "bootstrap-server-mapping", []string{}, "Mapping of Kafka bootstrap server address to local address (host:port,host:port(,advhost:advport))")
	Server.Flags().StringArrayVar(&externalServersMapping, "external-server-mapping", []string{}, "Mapping of Kafka server address to external address (host:port,host:port). A listener for the external address is not started")
	Server.Flags().StringArrayVar(&dialAddressMapping, "dial-address-mapping", []string{}, "Mapping of target broker address to new one (host:port,host:port). The mapping is performed during connection establishment")
	Server.Flags().BoolVar(&c.Proxy.DisableDynamicListeners, "dynamic-listeners-disable", false, "Disable dynamic listeners.")
	Server.Flags().IntVar(&c.Proxy.DynamicSequentialMinPort, "dynamic-sequential-min-port", 0, "If set to non-zero, makes the dynamic listener use a sequential port starting with this value rather than a random port every time.")

	Server.Flags().IntVar(&c.Proxy.RequestBufferSize, "proxy-request-buffer-size", 4096, "Request buffer size pro tcp connection")
	Server.Flags().IntVar(&c.Proxy.ResponseBufferSize, "proxy-response-buffer-size", 4096, "Response buffer size pro tcp connection")

	Server.Flags().IntVar(&c.Proxy.ListenerReadBufferSize, "proxy-listener-read-buffer-size", 0, "Size of the operating system's receive buffer associated with the connection. If zero, system default is used")
	Server.Flags().IntVar(&c.Proxy.ListenerWriteBufferSize, "proxy-listener-write-buffer-size", 0, "Sets the size of the operating system's transmit buffer associated with the connection. If zero, system default is used")
	Server.Flags().DurationVar(&c.Proxy.ListenerKeepAlive, "proxy-listener-keep-alive", 60*time.Second, "Keep alive period for an active network connection. If zero, keep-alives are disabled")

	Server.Flags().BoolVar(&c.Proxy.TLS.Enable, "proxy-listener-tls-enable", false, "Whether or not to use TLS listener")
	Server.Flags().StringVar(&c.Proxy.TLS.ListenerCertFile, "proxy-listener-cert-file", "", "PEM encoded file with server certificate")
	Server.Flags().StringVar(&c.Proxy.TLS.ListenerKeyFile, "proxy-listener-key-file", "", "PEM encoded file with private key for the server certificate")
	Server.Flags().StringVar(&c.Proxy.TLS.ListenerKeyPassword, "proxy-listener-key-password", "", "Password to decrypt rsa private key")
	Server.Flags().StringVar(&c.Proxy.TLS.CAChainCertFile, "proxy-listener-ca-chain-cert-file", "", "PEM encoded CA's certificate file. If provided, client certificate is required and verified")
	Server.Flags().StringSliceVar(&c.Proxy.TLS.ListenerCipherSuites, "proxy-listener-cipher-suites", []string{}, "List of supported cipher suites")
	Server.Flags().StringSliceVar(&c.Proxy.TLS.ListenerCurvePreferences, "proxy-listener-curve-preferences", []string{}, "List of curve preferences")

	// local authentication plugin
	Server.Flags().BoolVar(&c.Auth.Local.Enable, "auth-local-enable", false, "Enable local SASL/PLAIN authentication performed by listener - SASL handshake will not be passed to kafka brokers")
	Server.Flags().StringVar(&c.Auth.Local.Command, "auth-local-command", "", "Path to authentication plugin binary")
	Server.Flags().StringVar(&c.Auth.Local.Mechanism, "auth-local-mechanism", "PLAIN", "SASL mechanism used for local authentication: PLAIN or OAUTHBEARER")
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

	//Same TLS client cert
	Server.Flags().BoolVar(&c.Kafka.TLS.SameClientCertEnable, "same-client-cert-enable", false, "Use only when mutual TLS is enabled on proxy and broker. It controls whether a proxy validates if proxy client certificate matches brokers client cert (tls-client-cert-file)")

	// SASL by Proxy
	Server.Flags().BoolVar(&c.Kafka.SASL.Enable, "sasl-enable", false, "Connect using SASL")
	Server.Flags().StringVar(&c.Kafka.SASL.Username, "sasl-username", "", "SASL user name")
	Server.Flags().StringVar(&c.Kafka.SASL.Password, "sasl-password", "", "SASL user password")
	Server.Flags().StringVar(&c.Kafka.SASL.JaasConfigFile, "sasl-jaas-config-file", "", "Location of JAAS config file with SASL username and password")
	Server.Flags().StringVar(&c.Kafka.SASL.Method, "sasl-method", "PLAIN", "SASL method to use (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512")

	// SASL by Proxy plugin
	Server.Flags().BoolVar(&c.Kafka.SASL.Plugin.Enable, "sasl-plugin-enable", false, "Use plugin for SASL authentication")
	Server.Flags().StringVar(&c.Kafka.SASL.Plugin.Command, "sasl-plugin-command", "", "Path to authentication plugin binary")
	Server.Flags().StringVar(&c.Kafka.SASL.Plugin.Mechanism, "sasl-plugin-mechanism", "OAUTHBEARER", "SASL mechanism used for proxy authentication: PLAIN or OAUTHBEARER")
	Server.Flags().StringArrayVar(&c.Kafka.SASL.Plugin.Parameters, "sasl-plugin-param", []string{}, "Authentication plugin parameter")
	Server.Flags().StringVar(&c.Kafka.SASL.Plugin.LogLevel, "sasl-plugin-log-level", "trace", "Log level of the auth plugin")
	Server.Flags().DurationVar(&c.Kafka.SASL.Plugin.Timeout, "sasl-plugin-timeout", 10*time.Second, "Authentication timeout")

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
	Server.Flags().StringVar(&c.Log.LevelFieldName, "log-level-fieldname", "@level", "Log level fieldname for json format")
	Server.Flags().StringVar(&c.Log.TimeFiledName, "log-time-fieldname", "@timestamp", "Time fieldname for json format")
	Server.Flags().StringVar(&c.Log.MsgFiledName, "log-msg-fieldname", "@message", "Message fieldname for json format")

	// Connect through Socks5 or HTTP CONNECT to Kafka
	Server.Flags().StringVar(&c.ForwardProxy.Url, "forward-proxy", "", "URL of the forward proxy. Supported schemas are socks5 and http")

	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv() // read in environment variables that match
}

func Run(_ *cobra.Command, _ []string) {
	logrus.Infof("Starting kafka-proxy version %s", config.Version)

	var localPasswordAuthenticator apis.PasswordAuthenticator
	var localTokenAuthenticator apis.TokenInfo
	if c.Auth.Local.Enable {
		switch c.Auth.Local.Mechanism {
		case "PLAIN":
			var err error
			factory, ok := registry.GetComponent(new(apis.PasswordAuthenticatorFactory), c.Auth.Local.Command).(apis.PasswordAuthenticatorFactory)
			if ok {
				logrus.Infof("Using built-in '%s' PasswordAuthenticator for local PasswordAuthenticator", c.Auth.Local.Command)
				localPasswordAuthenticator, err = factory.New(c.Auth.Local.Parameters)
				if err != nil {
					logrus.Fatal(err)
				}
			} else {
				client := NewPluginClient(localauth.Handshake, localauth.PluginMap, c.Auth.Local.LogLevel, c.Auth.Local.Command, c.Auth.Local.Parameters)
				defer client.Kill()

				rpcClient, err := client.Client()
				if err != nil {
					logrus.Fatal(err)
				}
				raw, err := rpcClient.Dispense("passwordAuthenticator")
				if err != nil {
					logrus.Fatal(err)
				}
				localPasswordAuthenticator, ok = raw.(apis.PasswordAuthenticator)
				if !ok {
					logrus.Fatal(errors.New("unsupported PasswordAuthenticator plugin type"))
				}
			}
		case "OAUTHBEARER":
			var err error
			factory, ok := registry.GetComponent(new(apis.TokenInfoFactory), c.Auth.Local.Command).(apis.TokenInfoFactory)
			if ok {
				logrus.Infof("Using built-in '%s' TokenInfo for local TokenAuthenticator", c.Auth.Local.Command)

				localTokenAuthenticator, err = factory.New(c.Auth.Local.Parameters)
				if err != nil {
					logrus.Fatal(err)
				}
			} else {
				client := NewPluginClient(tokeninfo.Handshake, tokeninfo.PluginMap, c.Auth.Local.LogLevel, c.Auth.Local.Command, c.Auth.Local.Parameters)
				defer client.Kill()

				rpcClient, err := client.Client()
				if err != nil {
					logrus.Fatal(err)
				}
				raw, err := rpcClient.Dispense("tokenInfo")
				if err != nil {
					logrus.Fatal(err)
				}
				localTokenAuthenticator, ok = raw.(apis.TokenInfo)
				if !ok {
					logrus.Fatal(errors.New("unsupported TokenInfo plugin type"))
				}
			}
		default:
			logrus.Fatal(errors.New("unsupported local auth mechanism"))
		}
	}

	var saslTokenProvider apis.TokenProvider
	if c.Kafka.SASL.Plugin.Enable {
		switch c.Kafka.SASL.Plugin.Mechanism {
		case "OAUTHBEARER":
			var err error
			factory, ok := registry.GetComponent(new(apis.TokenProviderFactory), c.Kafka.SASL.Plugin.Command).(apis.TokenProviderFactory)
			if ok {
				logrus.Infof("Using built-in '%s' TokenProvider for sasl authentication", c.Kafka.SASL.Plugin.Command)

				saslTokenProvider, err = factory.New(c.Kafka.SASL.Plugin.Parameters)
				if err != nil {
					logrus.Fatal(err)
				}
			} else {
				client := NewPluginClient(tokenprovider.Handshake, tokenprovider.PluginMap, c.Kafka.SASL.Plugin.LogLevel, c.Kafka.SASL.Plugin.Command, c.Kafka.SASL.Plugin.Parameters)
				defer client.Kill()

				rpcClient, err := client.Client()
				if err != nil {
					logrus.Fatal(err)
				}
				raw, err := rpcClient.Dispense("tokenProvider")
				if err != nil {
					logrus.Fatal(err)
				}
				saslTokenProvider, ok = raw.(apis.TokenProvider)
				if !ok {
					logrus.Fatal(errors.New("unsupported TokenProvider plugin type"))
				}
			}
		default:
			logrus.Fatal(errors.New("unsupported sasl auth mechanism"))
		}
	}

	var gatewayTokenProvider apis.TokenProvider
	if c.Auth.Gateway.Client.Enable {
		var err error
		factory, ok := registry.GetComponent(new(apis.TokenProviderFactory), c.Auth.Gateway.Client.Command).(apis.TokenProviderFactory)
		if ok {
			logrus.Infof("Using built-in '%s' TokenProvider for Gateway Client", c.Auth.Gateway.Client.Command)
			gatewayTokenProvider, err = factory.New(c.Auth.Gateway.Client.Parameters)
			if err != nil {
				logrus.Fatal(err)
			}
		} else {
			client := NewPluginClient(tokenprovider.Handshake, tokenprovider.PluginMap, c.Auth.Gateway.Client.LogLevel, c.Auth.Gateway.Client.Command, c.Auth.Gateway.Client.Parameters)
			defer client.Kill()

			rpcClient, err := client.Client()
			if err != nil {
				logrus.Fatal(err)
			}
			raw, err := rpcClient.Dispense("tokenProvider")
			if err != nil {
				logrus.Fatal(err)
			}
			gatewayTokenProvider, ok = raw.(apis.TokenProvider)
			if !ok {
				logrus.Fatal(errors.New("unsupported TokenProvider plugin type"))
			}
		}
	}

	var gatewayTokenInfo apis.TokenInfo
	if c.Auth.Gateway.Server.Enable {
		var err error
		factory, ok := registry.GetComponent(new(apis.TokenInfoFactory), c.Auth.Gateway.Server.Command).(apis.TokenInfoFactory)
		if ok {
			logrus.Infof("Using built-in '%s' TokenInfo for Gateway Server", c.Auth.Gateway.Server.Command)

			gatewayTokenInfo, err = factory.New(c.Auth.Gateway.Server.Parameters)
			if err != nil {
				logrus.Fatal(err)
			}
		} else {
			client := NewPluginClient(tokeninfo.Handshake, tokeninfo.PluginMap, c.Auth.Gateway.Server.LogLevel, c.Auth.Gateway.Server.Command, c.Auth.Gateway.Server.Parameters)
			defer client.Kill()

			rpcClient, err := client.Client()
			if err != nil {
				logrus.Fatal(err)
			}
			raw, err := rpcClient.Dispense("tokenInfo")
			if err != nil {
				logrus.Fatal(err)
			}
			gatewayTokenInfo, ok = raw.(apis.TokenInfo)
			if !ok {
				logrus.Fatal(errors.New("unsupported TokenInfo plugin type"))
			}
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
		proxyClient, err := proxy.NewClient(connset, c, listeners.GetNetAddressMapping, localPasswordAuthenticator, localTokenAuthenticator, saslTokenProvider, gatewayTokenProvider, gatewayTokenInfo)
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
				logrus.FieldKeyTime:  c.Log.TimeFiledName,
				logrus.FieldKeyLevel: c.Log.LevelFieldName,
				logrus.FieldKeyMsg:   c.Log.MsgFiledName,
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
