package tlsserver

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/grepplabs/cert-source/tls/keyutil"
	"github.com/grepplabs/cert-source/tls/server/source"
)

const (
	initLoadTimeout = 5 * time.Second
)

// MustNewServerConfig is like NewServerConfig but panics if the config cannot be created.
func MustNewServerConfig(logger *slog.Logger, src source.ServerCertsSource, opts ...TLSServerConfigOption) *tls.Config {
	c, err := NewServerConfig(logger, src, opts...)
	if err != nil {
		panic(`tls: NewServerConfig(): ` + err.Error())
	}
	return c
}

// NewServerConfig provides new server TLS configuration.
func NewServerConfig(logger *slog.Logger, src source.ServerCertsSource, opts ...TLSServerConfigOption) (*tls.Config, error) {
	store, err := NewServerCertsStore(logger, src)
	if err != nil {
		return nil, err
	}
	tlsConfig := tls.Config{
		GetConfigForClient: func(info *tls.ClientHelloInfo) (*tls.Config, error) {
			cs := store.LoadServerCerts()
			x := &tls.Config{
				MinVersion:   tls.VersionTLS12,
				Certificates: cs.Certificates,
			}
			if cs.ClientCAs != nil {
				x.ClientCAs = cs.ClientCAs
				x.ClientAuth = tls.RequireAndVerifyClientCert
				x.VerifyPeerCertificate = verifyClientCertificate(logger, store)
			}
			for _, opt := range opts {
				opt(x)
			}
			return x, nil
		},
	}
	// ignored as GetConfigForClient is used. it is only required to invoke http.ListenAndServeTLS("", "")
	cs := store.LoadServerCerts()
	tlsConfig.Certificates = cs.Certificates
	if cs.ClientCAs != nil {
		tlsConfig.ClientCAs = cs.ClientCAs
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		tlsConfig.VerifyPeerCertificate = verifyClientCertificate(logger, store)
	}
	for _, opt := range opts {
		opt(&tlsConfig)
	}
	return &tlsConfig, nil
}

func NewServerCertsStore(logger *slog.Logger, src source.ServerCertsSource) (*source.ServerCertsStore, error) {
	store := source.NewServerCertsStore(logger)
	logger.Info("initial server certs loading")

	certsChan := src.ServerCerts()

	select {
	case certs := <-certsChan:
		store.SetServerCerts(certs)
	case <-time.After(initLoadTimeout):
		return nil, errors.New("get server certs timeout")
	}

	go func() {
		for certs := range certsChan {
			store.SetServerCerts(certs)
		}
	}()
	return store, nil
}

func verifyClientCertificate(logger *slog.Logger, store *source.ServerCertsStore) func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		cs := store.LoadServerCerts()
		if len(cs.ClientCRLs) == 0 {
			return nil
		}
		for _, chain := range verifiedChains {
			for _, cert := range chain {
				if !cert.IsCA {
					if cs.IsClientCertRevoked(cert.SerialNumber) {
						err := fmt.Errorf("client certificte %s was revoked", keyutil.GetHexFormatted(cert.SerialNumber.Bytes(), ":"))
						logger.Debug(err.Error())
						return err
					}
				}
			}
		}
		return nil
	}
}
