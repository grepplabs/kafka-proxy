package proxy

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"github.com/grepplabs/kafka-proxy/config"
	"github.com/pkg/errors"
	"io/ioutil"
)

func newTLSListenerConfig(conf *config.Config) (*tls.Config, error) {
	opts := conf.Proxy.TLS

	if opts.ListenerKeyFile == "" || opts.ListenerCertFile == "" {
		return nil, errors.New("Listener key and cert files must not be empty")
	}
	certPEMBlock, err := ioutil.ReadFile(opts.ListenerCertFile)
	if err != nil {
		return nil, err
	}
	keyPEMBlock, err := ioutil.ReadFile(opts.ListenerKeyFile)
	if err != nil {
		return nil, err
	}
	keyPEMBlock, err = decryptPEM(keyPEMBlock, opts.ListenerKeyPassword)
	if err != nil {
		return nil, err
	}
	cert, err := tls.X509KeyPair(certPEMBlock, keyPEMBlock)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.NoClientCert,
	}
	if opts.CAChainCertFile != "" {
		caCertPEMBlock, err := ioutil.ReadFile(opts.CAChainCertFile)
		if err != nil {
			return nil, err
		}
		clientCAs := x509.NewCertPool()
		if ok := clientCAs.AppendCertsFromPEM(caCertPEMBlock); !ok {
			return nil, errors.New("Failed to parse listener root certificate")
		}
		cfg.ClientCAs = clientCAs
	}
	return cfg, nil
}

func newTLSClientConfig(conf *config.Config) (*tls.Config, error) {
	opts := conf.Kafka.TLS

	cfg := &tls.Config{InsecureSkipVerify: opts.InsecureSkipVerify}

	if opts.ClientCertFile != "" && opts.ClientKeyFile != "" {
		certPEMBlock, err := ioutil.ReadFile(opts.ClientCertFile)
		if err != nil {
			return nil, err
		}
		keyPEMBlock, err := ioutil.ReadFile(opts.ClientKeyFile)
		if err != nil {
			return nil, err
		}
		keyPEMBlock, err = decryptPEM(keyPEMBlock, opts.ClientKeyPassword)
		if err != nil {
			return nil, err
		}
		cert, err := tls.X509KeyPair(certPEMBlock, keyPEMBlock)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
		cfg.BuildNameToCertificate()
	}

	if opts.CAChainCertFile != "" {
		caCertPEMBlock, err := ioutil.ReadFile(opts.CAChainCertFile)
		if err != nil {
			return nil, err
		}
		rootCAs := x509.NewCertPool()
		if ok := rootCAs.AppendCertsFromPEM(caCertPEMBlock); !ok {
			return nil, errors.New("Failed to parse client root certificate")
		}

		cfg.RootCAs = rootCAs
	}
	return cfg, nil
}

func decryptPEM(pemData []byte, password string) ([]byte, error) {

	keyBlock, _ := pem.Decode(pemData)
	if keyBlock == nil {
		return nil, errors.New("Failed to parse PEM")
	}
	if x509.IsEncryptedPEMBlock(keyBlock) {
		if password == "" {
			return nil, errors.New("PEM is encrypted, but password is empty")
		}
		key, err := x509.DecryptPEMBlock(keyBlock, []byte(password))
		if err != nil {
			return nil, err
		}
		block := &pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: key,
		}
		return pem.EncodeToMemory(block), nil
	}
	return pemData, nil
}
