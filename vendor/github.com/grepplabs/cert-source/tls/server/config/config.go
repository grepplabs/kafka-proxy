package config

import (
	"crypto/tls"
	"fmt"
	"log/slog"

	"github.com/grepplabs/cert-source/config"
	tlsserver "github.com/grepplabs/cert-source/tls/server"
	"github.com/grepplabs/cert-source/tls/server/filesource"
)

func GetServerTLSConfig(logger *slog.Logger, conf *config.TLSServerConfig, opts ...tlsserver.TLSServerConfigOption) (*tls.Config, error) {
	fs, err := filesource.New(
		filesource.WithLogger(logger),
		filesource.WithX509KeyPair(conf.File.Cert, conf.File.Key),
		filesource.WithClientAuthFile(conf.File.ClientCAs),
		filesource.WithClientCRLFile(conf.File.ClientCRL),
		filesource.WithRefresh(conf.Refresh),
		filesource.WithKeyPassword(conf.KeyPassword),
	)
	if err != nil {
		return nil, fmt.Errorf("setup server cert file source: %w", err)
	}
	tlsConfig, err := tlsserver.NewServerConfig(logger, fs, opts...)
	if err != nil {
		return nil, fmt.Errorf("setup server TLS config: %w", err)
	}
	return tlsConfig, nil
}
