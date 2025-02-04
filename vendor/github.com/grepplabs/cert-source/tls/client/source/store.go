package source

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"sync/atomic"
)

type ClientCertsSource interface {
	ClientCerts() chan ClientCerts
}

type ClientCerts struct {
	InsecureSkipVerify bool
	Certificate        *tls.Certificate
	RootCAs            *x509.CertPool
	Checksum           []byte
}

func (s ClientCerts) GetChecksum() []byte {
	return s.Checksum
}

type ClientCertsStore struct {
	cs     atomic.Pointer[ClientCerts]
	logger *slog.Logger
}

func NewClientCertsStore(logger *slog.Logger) *ClientCertsStore {
	s := &ClientCertsStore{
		logger: logger,
	}
	s.cs.Store(&ClientCerts{})
	return s
}

func (s *ClientCertsStore) LoadClientCerts() ClientCerts {
	return *s.cs.Load()
}

func (s *ClientCertsStore) SetClientCerts(certs ClientCerts) {
	s.cs.Store(&certs)
	s.logger.Info(fmt.Sprintf("stored x509 client root certs, client cert [%s]", name(certs.Certificate)))
}

func name(cert *tls.Certificate) string {
	if cert == nil {
		return ""
	}
	x509Cert, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%s=%s", getHexFormatted(x509Cert.SerialNumber.Bytes(), ":"), x509Cert.Subject.CommonName)
}

func getHexFormatted(buf []byte, sep string) string {
	var ret bytes.Buffer
	for _, cur := range buf {
		if ret.Len() > 0 {
			_, _ = fmt.Fprint(&ret, sep)
		}
		_, _ = fmt.Fprintf(&ret, "%02x", cur)
	}
	return ret.String()
}
