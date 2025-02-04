package proxy

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"log/slog"
	"net"
	"os"
	"reflect"
	"strings"
	"time"

	tlsconfig "github.com/grepplabs/cert-source/config"
	tlsclientconfig "github.com/grepplabs/cert-source/tls/client/config"
	tlsserver "github.com/grepplabs/cert-source/tls/server"
	tlsserverconfig "github.com/grepplabs/cert-source/tls/server/config"
	"github.com/grepplabs/kafka-proxy/config"
	"github.com/pkg/errors"
)

var (
	supportedCurvesMap = map[string]tls.CurveID{
		"X25519": tls.X25519,
		"P256":   tls.CurveP256,
		"P384":   tls.CurveP384,
		"P521":   tls.CurveP521,
	}
	supportedCiphersMap = map[string]uint16{
		"TLS_RSA_WITH_RC4_128_SHA":                      tls.TLS_RSA_WITH_RC4_128_SHA,
		"TLS_RSA_WITH_3DES_EDE_CBC_SHA":                 tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,
		"TLS_RSA_WITH_AES_128_CBC_SHA":                  tls.TLS_RSA_WITH_AES_128_CBC_SHA,
		"TLS_RSA_WITH_AES_256_CBC_SHA":                  tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		"TLS_RSA_WITH_AES_128_CBC_SHA256":               tls.TLS_RSA_WITH_AES_128_CBC_SHA256,
		"TLS_RSA_WITH_AES_128_GCM_SHA256":               tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
		"TLS_RSA_WITH_AES_256_GCM_SHA384":               tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
		"TLS_ECDHE_ECDSA_WITH_RC4_128_SHA":              tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA,
		"TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA":          tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
		"TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA":          tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
		"TLS_ECDHE_RSA_WITH_RC4_128_SHA":                tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA,
		"TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA":           tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA,
		"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA":            tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
		"TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA":            tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		"TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256":       tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
		"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256":         tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
		"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256":         tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256":       tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384":         tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384":       tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		"TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256":   tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
		"TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256": tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
		"TLS_AES_128_GCM_SHA256":                        tls.TLS_AES_128_GCM_SHA256,
		"TLS_AES_256_GCM_SHA384":                        tls.TLS_AES_256_GCM_SHA384,
		"TLS_CHACHA20_POLY1305_SHA256":                  tls.TLS_CHACHA20_POLY1305_SHA256,
	}
	zeroTime = time.Time{}
)

func newTLSListenerConfig(conf *config.Config) (*tls.Config, error) {
	opts := conf.Proxy.TLS

	cipherSuites, err := getCipherSuites(opts.ListenerCipherSuites)
	if err != nil {
		return nil, err
	}
	curvePreferences, err := getCurvePreferences(opts.ListenerCurvePreferences)
	if err != nil {
		return nil, err
	}
	tlsValidateFunc, err := tlsClientCertVerificationFunc(conf)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := tlsserverconfig.GetServerTLSConfig(slog.Default(),
		&tlsconfig.TLSServerConfig{
			Enable:      true,
			Refresh:     opts.Refresh,
			KeyPassword: opts.ListenerKeyPassword,
			File: tlsconfig.TLSServerFiles{
				Key:       opts.ListenerKeyFile,
				Cert:      opts.ListenerCertFile,
				ClientCAs: opts.ListenerCAChainCertFile,
				ClientCRL: opts.ListenerCRLFile,
			},
		},
		tlsserver.WithTLSServerVerifyPeerCertificate(tlsValidateFunc),
		tlsserver.WithTLSServerCipherSuites(cipherSuites),
		tlsserver.WithTLSServerCurvePreferences(curvePreferences),
	)
	if err != nil {
		return nil, err
	}
	return tlsConfig, nil
}

func getCipherSuites(enabledCipherSuites []string) ([]uint16, error) {
	suites := make([]uint16, 0)
	for _, suite := range enabledCipherSuites {
		cipher, ok := supportedCiphersMap[strings.TrimSpace(suite)]
		if !ok {
			return nil, errors.Errorf("invalid cipher suite '%s' selected, supported ciphers %v", suite, reflect.ValueOf(supportedCiphersMap).MapKeys())
		}
		suites = append(suites, cipher)
	}
	if len(suites) == 0 {
		return nil, nil
	}
	return suites, nil
}

func getCurvePreferences(enabledCurvePreferences []string) ([]tls.CurveID, error) {
	curvePreferences := make([]tls.CurveID, 0)
	for _, curveID := range enabledCurvePreferences {
		curvePreference, ok := supportedCurvesMap[strings.TrimSpace(curveID)]
		if !ok {
			return nil, errors.Errorf("invalid curveID '%s' selected, supported curveIDs %v", curveID, reflect.ValueOf(supportedCurvesMap).MapKeys())
		}
		curvePreferences = append(curvePreferences, curvePreference)
	}
	if len(curvePreferences) == 0 {
		return nil, nil
	}
	return curvePreferences, nil
}

type TLSConfigFunc func() *tls.Config

func newTLSClientConfig(conf *config.Config) (TLSConfigFunc, error) {
	// https://blog.cloudflare.com/exposing-go-on-the-internet/
	opts := conf.Kafka.TLS
	tlsConfigFunc, err := tlsclientconfig.GetTLSClientConfigFunc(slog.Default(), &tlsconfig.TLSClientConfig{
		Enable:             true,
		Refresh:            opts.Refresh,
		InsecureSkipVerify: opts.InsecureSkipVerify,
		KeyPassword:        opts.ClientKeyPassword,
		UseSystemPool:      opts.SystemCertPool,
		File: tlsconfig.TLSClientFiles{
			Key:     opts.ClientKeyFile,
			Cert:    opts.ClientCertFile,
			RootCAs: opts.CAChainCertFile,
		},
	})
	if err != nil {
		return nil, err
	}
	return func() *tls.Config {
		return tlsConfigFunc()
	}, err
}

func parseCertificate(certFile string) (*x509.Certificate, error) {

	content, readErr := os.ReadFile(certFile)

	if readErr != nil {
		return nil, errors.Errorf("Failed to read file from location '%s'", certFile)
	}

	block, _ := pem.Decode(content)

	cert, parseErr := x509.ParseCertificate(block.Bytes)

	if parseErr != nil {
		return nil, errors.Errorf("Failed to parse certificate file from location '%s'", certFile)
	}

	return cert, nil
}

func handshakeAsTLSAndValidateClientCert(conn net.Conn, expectedCert *x509.Certificate, handshakeTimeout time.Duration) error {
	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		return errors.New("Unable to cast connection to TLS when validating client cert")
	}

	err := handshakeTLSConn(tlsConn, handshakeTimeout)
	if err != nil {
		return err
	}

	actualClientCert := filterClientCertificate(tlsConn.ConnectionState().PeerCertificates)

	result := validateClientCert(actualClientCert, expectedCert)

	return result
}

func handshakeTLSConn(tlsConn *tls.Conn, timeout time.Duration) error {
	err := tlsConn.SetDeadline(time.Now().Add(timeout))
	if err != nil {
		return errors.Errorf("Failed to set deadline with handshake timeout in seconds %f on connection: %v", timeout.Seconds(), err)
	}

	err = tlsConn.Handshake()
	if err != nil {
		return errors.Errorf("TLS handshake failed when exchanging client certificates: %v", err)
	}

	err = tlsConn.SetDeadline(zeroTime)
	if err != nil {
		return errors.Errorf("Failed to reset deadline on connection: %v", err)
	}

	return err
}

func filterClientCertificate(peerCertificates []*x509.Certificate) *x509.Certificate {
	for _, v := range peerCertificates {
		if !v.IsCA {
			return v
		}
	}
	return nil
}

func validateClientCert(actualClientCert *x509.Certificate, expectedCert *x509.Certificate) error {
	if actualClientCert == nil {
		return errors.New("Client cert not found in TLS connection")
	}

	if !actualClientCert.Equal(expectedCert) {
		return errors.New("Client cert sent by proxy client does not match brokers client cert (tls-client-cert-file)")
	}
	return nil
}
