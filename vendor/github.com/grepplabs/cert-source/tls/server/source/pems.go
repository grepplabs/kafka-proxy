package source

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"

	"github.com/grepplabs/cert-source/tls/keyutil"
)

type ServerPEMsLoader interface {
	Load() (*ServerPEMs, error)
}

type ServerPEMs struct {
	CertPEMBlock       []byte
	KeyPEMBlock        []byte
	ClientAuthPEMBlock []byte
	CRLPEMBlock        []byte
}

func (s ServerPEMs) Checksum() []byte {
	hash := sha256.New()
	hash.Write(s.CertPEMBlock)
	hash.Write(s.KeyPEMBlock)
	hash.Write(s.ClientAuthPEMBlock)
	return hash.Sum(s.CRLPEMBlock)
}

func (s ServerPEMs) Certificates() ([]tls.Certificate, error) {
	cert, err := tls.X509KeyPair(s.CertPEMBlock, s.KeyPEMBlock)
	if err != nil {
		return nil, err
	}
	return []tls.Certificate{cert}, nil
}

func (s ServerPEMs) ClientCAs() (*x509.CertPool, error) {
	if len(s.ClientAuthPEMBlock) == 0 {
		return nil, nil
	}
	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(s.ClientAuthPEMBlock) {
		return nil, errors.New("server PEMs: building client CAs failed")
	}
	return certPool, nil
}

func (s ServerPEMs) ClientCRLs() ([]*x509.RevocationList, error) {
	if len(s.CRLPEMBlock) == 0 {
		return nil, nil
	}
	return keyutil.ParseCRLsPEM(s.CRLPEMBlock)
}

func (s ServerPEMs) ValidateCRLs() error {
	if len(s.ClientAuthPEMBlock) == 0 {
		return nil
	}
	clientCRLs, err := s.ClientCRLs()
	if err != nil {
		return err
	}
	if len(clientCRLs) == 0 {
		return nil
	}
	certs, err := keyutil.ParseCertsPEM(s.ClientAuthPEMBlock)
	if err != nil {
		return err
	}
	for _, clientCRL := range clientCRLs {
		ok := false
		for _, cert := range certs {
			err := clientCRL.CheckSignatureFrom(cert)
			if err == nil {
				ok = true
				continue
			}
		}
		if !ok {
			return errors.New("server PEMs: CRL validation failure")
		}
	}
	return nil
}
