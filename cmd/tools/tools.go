package tools

import (
	"github.com/armon/go-socks5"
	"github.com/elazarl/goproxy"
	"github.com/elazarl/goproxy/ext/auth"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"net/http"
)

var Tools = &cobra.Command{
	Use:   "tools",
	Short: "Tools",
}

var httpProxy = &cobra.Command{
	Use:   "http-proxy",
	Short: "HTTP proxy server",
	RunE:  httpProxyServer,
}

var socks5Proxy = &cobra.Command{
	Use:   "socks5-proxy",
	Short: "SOCKS5 proxy server",
	RunE:  socks5ProxyServer,
}

func init() {
	Tools.AddCommand(httpProxy)
	Tools.AddCommand(socks5Proxy)

	Tools.PersistentFlags().String("username", "", `username for proxy authentication`)
	Tools.PersistentFlags().String("password", "", "password for proxy authentication")

	httpProxy.Flags().String("addr", ":3128", "proxy listen address")
	httpProxy.Flags().Bool("verbose", false, "should every proxy request be logged to stdout")

	socks5Proxy.Flags().String("addr", ":1080", "proxy listen address")
}

func httpProxyServer(cmd *cobra.Command, _ []string) error {
	username, _ := cmd.Flags().GetString("username")
	password, _ := cmd.Flags().GetString("password")
	addr, _ := cmd.Flags().GetString("addr")
	verbose, _ := cmd.Flags().GetBool("verbose")

	proxy := goproxy.NewProxyHttpServer()
	proxy.Verbose = verbose
	if username != "" && password != "" {
		logrus.Info("HTTP proxy will require basic authentication")

		proxy.OnRequest().HandleConnect(auth.BasicConnect("", func(user, passwd string) bool {
			return user == username && passwd == password
		}))
	}

	logrus.Infof("Starting HTTP proxy server on %s", addr)
	return http.ListenAndServe(addr, proxy)
}

func socks5ProxyServer(cmd *cobra.Command, _ []string) error {
	username, _ := cmd.Flags().GetString("username")
	password, _ := cmd.Flags().GetString("password")
	addr, _ := cmd.Flags().GetString("addr")

	conf := &socks5.Config{}
	server, err := socks5.New(conf)
	if err != nil {
		return err
	}
	if username != "" && password != "" {
		logrus.Info("SOCKS5 proxy will require basic authentication", addr)

		authenticator := &socks5.UserPassAuthenticator{
			Credentials: socks5ProxyCredentials{
				username: username,
				password: password,
			},
		}
		conf.AuthMethods = []socks5.Authenticator{authenticator}
	}

	logrus.Infof("Starting SOCKS5 proxy server on %s", addr)
	return server.ListenAndServe("tcp", addr)
}

type socks5ProxyCredentials struct {
	username, password string
}

func (s socks5ProxyCredentials) Valid(username, password string) bool {
	return s.username == username && s.password == password
}
