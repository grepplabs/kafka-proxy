package filesource

import (
	"errors"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	tlscert "github.com/grepplabs/cert-source/tls/client/source"
	"github.com/grepplabs/cert-source/tls/keyutil"
	"github.com/grepplabs/cert-source/tls/watcher"
)

type fileSource struct {
	insecureSkipVerify bool
	certFile           string
	keyFile            string
	keyPassword        string
	rootCAsFile        string
	useSystemPool      bool
	refresh            time.Duration
	logger             *slog.Logger
	notifyFunc         func()
	lastClientCerts    atomic.Pointer[tlscert.ClientCerts]
}

func New(opts ...Option) (tlscert.ClientCertsSource, error) {
	s := &fileSource{
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(s)
	}
	lastClientCerts, err := s.getClientCerts()
	if err != nil {
		return nil, err
	}
	s.lastClientCerts.Store(lastClientCerts)
	return s, nil
}

func MustNew(opts ...Option) tlscert.ClientCertsSource {
	serverSource, err := New(opts...)
	if err != nil {
		panic(`filesource: New(): ` + err.Error())
	}
	return serverSource
}

func (s *fileSource) getClientCerts() (*tlscert.ClientCerts, error) {
	pemBlocks, err := s.Load()
	if err != nil {
		return nil, err
	}
	certificate, err := pemBlocks.Certificate()
	if err != nil {
		return nil, err
	}
	rootCAs, err := pemBlocks.RootCAs()
	if err != nil {
		return nil, err
	}
	return &tlscert.ClientCerts{
		InsecureSkipVerify: s.insecureSkipVerify,
		Certificate:        certificate,
		RootCAs:            rootCAs,
		Checksum:           pemBlocks.Checksum(),
	}, nil
}

func (s *fileSource) refreshClientCerts() (*tlscert.ClientCerts, error) {
	clientCerts, err := s.getClientCerts()
	if err != nil {
		return nil, err
	}
	s.lastClientCerts.Store(clientCerts)
	return clientCerts, nil
}

func (s *fileSource) ClientCerts() chan tlscert.ClientCerts {
	initialClientCert := s.lastClientCerts.Load()
	ch := make(chan tlscert.ClientCerts, 1)
	if initialClientCert != nil {
		ch <- *initialClientCert
	}
	if s.refresh <= 0 {
		close(ch)
	} else {
		go func() {
			watcher.Watch(s.logger, ch, s.refresh, initialClientCert, s.refreshClientCerts, s.notifyFunc)
			close(ch)
		}()
	}
	return ch
}

func (s *fileSource) Load() (pemBlocks *tlscert.ClientPEMs, err error) {
	pemBlocks = &tlscert.ClientPEMs{
		UseSystemPool: s.useSystemPool,
	}
	if (s.certFile == "") != (s.keyFile == "") {
		return nil, errors.New("cert file source: both certFile and keyFile must be set or be empty")
	}
	if s.certFile != "" && s.keyFile != "" {
		if pemBlocks.CertPEMBlock, err = s.readFile(s.certFile); err != nil {
			return nil, err
		}
		if pemBlocks.KeyPEMBlock, err = s.readFile(s.keyFile); err != nil {
			return nil, err
		}
		if pemBlocks.KeyPEMBlock, err = keyutil.DecryptPrivateKeyPEM(pemBlocks.KeyPEMBlock, s.keyPassword); err != nil {
			return nil, err
		}
	}
	if pemBlocks.RootCAsPEMBlock, err = s.readFile(s.rootCAsFile); err != nil {
		return nil, err
	}
	return pemBlocks, nil
}

func (s *fileSource) readFile(name string) ([]byte, error) {
	if name == "" {
		return nil, nil
	}
	return os.ReadFile(name)
}
