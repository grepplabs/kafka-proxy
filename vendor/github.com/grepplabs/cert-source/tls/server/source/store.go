package source

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"sync/atomic"

	"github.com/grepplabs/cert-source/tls/keyutil"
)

type ServerCertsSource interface {
	ServerCerts() chan ServerCerts
}

type ServerCerts struct {
	Certificates         []tls.Certificate
	ClientCAs            *x509.CertPool
	ClientCRLs           []*x509.RevocationList
	Checksum             []byte
	RevokedSerialNumbers map[string]struct{}
}

func (s *ServerCerts) GetChecksum() []byte {
	return s.Checksum
}

func NewRevokedSerialNumbers(clientCRLs []*x509.RevocationList) map[string]struct{} {
	revokedSerialNumbers := make(map[string]struct{})
	for _, clientCRL := range clientCRLs {
		for _, revoked := range clientCRL.RevokedCertificateEntries {
			revokedSerialNumbers[string(revoked.SerialNumber.Bytes())] = struct{}{}
		}
	}
	return revokedSerialNumbers
}

func (s *ServerCerts) IsClientCertRevoked(serialNumber *big.Int) bool {
	_, ok := s.RevokedSerialNumbers[string(serialNumber.Bytes())]
	return ok
}

type ServerCertsStore struct {
	cs     atomic.Pointer[ServerCerts]
	logger *slog.Logger
}

func NewServerCertsStore(logger *slog.Logger) *ServerCertsStore {
	s := &ServerCertsStore{
		logger: logger,
	}
	s.cs.Store(&ServerCerts{})
	return s
}

func (s *ServerCertsStore) LoadServerCerts() ServerCerts {
	return *s.cs.Load()
}

func (s *ServerCertsStore) SetServerCerts(certs ServerCerts) {
	s.cs.Store(&certs)
	s.logger.Info(fmt.Sprintf("stored x509 server certs for names [%s]", names(certs.Certificates)))
}

func names(certs []tls.Certificate) []string {
	var result []string
	for _, c := range certs {
		x509Cert, err := x509.ParseCertificate(c.Certificate[0])
		if err != nil {
			continue
		}
		var names []string
		if len(x509Cert.Subject.CommonName) > 0 {
			names = append(names, x509Cert.Subject.CommonName)
		}
		names = append(names, x509Cert.DNSNames...)
		for _, ip := range x509Cert.IPAddresses {
			names = append(names, ip.String())
		}
		result = append(result, fmt.Sprintf("%s=%s", keyutil.GetHexFormatted(x509Cert.SerialNumber.Bytes(), ":"), strings.Join(names, ",")))
	}
	return result
}
