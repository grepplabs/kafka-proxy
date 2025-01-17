package proxy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"
)

type SchemaRegistryProxy struct {
	url        string
	username   string
	password   string
	port       int
	proxyPort  int
	httpServer *http.Server
}

func NewSchemaRegistryProxy(url, username, password string, port, proxyPort int) (*SchemaRegistryProxy, error) {
	if err := validateSchemaRegistryCreds(username, password); err != nil {
		return nil, err
	}

	return &SchemaRegistryProxy{
		url:       url,
		username:  username,
		password:  password,
		port:      port,
		proxyPort: proxyPort,
	}, nil
}

func validateSchemaRegistryCreds(username, password string) error {
	if username == "" || password == "" {
		return fmt.Errorf("schema Registry proxy requires both username and password")
	}
	return nil
}

func (s *SchemaRegistryProxy) Start() error {
	remote, err := url.Parse(fmt.Sprintf("https://%s:%d", s.url, s.port))
	if err != nil {
		return fmt.Errorf("invalid Schema Registry URL: %w", err)
	}

	proxy := httputil.NewSingleHostReverseProxy(remote)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		logrus.WithError(err).Error("Schema Registry proxy error")
		w.WriteHeader(http.StatusBadGateway)
	}

	// Setup proxy handler with logging and auth
	handler := s.createProxyHandler(proxy, remote)

	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%d", s.proxyPort),
		Handler:      handler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	logrus.WithFields(logrus.Fields{
		"listen_port": s.proxyPort,
		"target_url":  remote.String(),
	}).Info("Starting Schema Registry proxy")

	if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(http.ErrServerClosed, err) {
		return fmt.Errorf("schema Registry proxy server error: %w", err)
	}
	return nil
}

func (s *SchemaRegistryProxy) Stop(ctx context.Context) error {
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}

func (s *SchemaRegistryProxy) createProxyHandler(proxy *httputil.ReverseProxy, remote *url.URL) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logrus.WithFields(logrus.Fields{
			"method": r.Method,
			"path":   r.URL.Path,
		}).Debug("Schema Registry proxy request")

		r.Host = remote.Host
		r.SetBasicAuth(s.username, s.password)

		proxy.ServeHTTP(w, r)
	})
}
func ExposeSchemaRegistry(schemaRegistryUrl, schemaRegistryUsername, schemaRegistryPassword string, schemaRegistryPort, schemaRegistryProxyPort int) {
	proxy, err := NewSchemaRegistryProxy(
		schemaRegistryUrl,
		schemaRegistryUsername,
		schemaRegistryPassword,
		schemaRegistryPort,
		schemaRegistryProxyPort,
	)
	if err != nil {
		logrus.Fatal(err)
	}

	if err := proxy.Start(); err != nil {
		logrus.Fatal(err)
	}
}

// func ExposeSchemaRegistry(schemaRegistryUrl, schemaRegistryUsername, schemaRegistryPassword string, schemaRegistryPort, schemaRegistryProxyPort int) {
// 	e := validateCreds(schemaRegistryUsername, schemaRegistryPassword)
// 	if e != nil {
// 		panic(e)
// 	}
//
// 	remote, err := url.Parse(fmt.Sprintf("https://%s:%d", schemaRegistryUrl, schemaRegistryPort))
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	handler := func(p *httputil.ReverseProxy) func(w http.ResponseWriter, r *http.Request) {
// 		return func(w http.ResponseWriter, r *http.Request) {
// 			logrus.Infof("Schema Registry port: %s", r.URL)
// 			r.Host = remote.Host
// 			r.SetBasicAuth(schemaRegistryUsername, schemaRegistryPassword)
// 			p.ServeHTTP(w, r)
//
// 		}
// 	}
//
// 	proxy := httputil.NewSingleHostReverseProxy(remote)
// 	http.HandleFunc("/", handler(proxy))
// 	err = http.ListenAndServe(fmt.Sprintf(":%d", schemaRegistryProxyPort), nil)
// 	if err != nil {
// 		panic(err)
// 	}
// }
