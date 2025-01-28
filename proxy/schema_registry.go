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

func validateSchemaRegistryCreds(username, password string) error {
	if username == "" || password == "" {
		return fmt.Errorf("schema Registry proxy requires both username and password")
	}
	return nil
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
