package config

import (
	"fmt"
	"log/slog"

	"github.com/grepplabs/cert-source/config"
	tlsclient "github.com/grepplabs/cert-source/tls/client"
	"github.com/grepplabs/cert-source/tls/client/filesource"
)

func GetTLSClientConfigFunc(logger *slog.Logger, conf *config.TLSClientConfig, opts ...tlsclient.TLSClientConfigOption) (tlsclient.TLSClientConfigFunc, error) {
	if !conf.Enable {
		return nil, nil
	}
	fs, err := filesource.New(
		filesource.WithLogger(logger.With("tls", "client")),
		filesource.WithRefresh(conf.Refresh),
		filesource.WithInsecureSkipVerify(conf.InsecureSkipVerify),
		filesource.WithClientCert(conf.File.Cert, conf.File.Key),
		filesource.WithClientRootCAs(conf.File.RootCAs),
		filesource.WithKeyPassword(conf.KeyPassword),
		filesource.WithSystemPool(conf.UseSystemPool),
	)
	if err != nil {
		return nil, fmt.Errorf("setup client cert file source: %w", err)
	}
	return tlsclient.NewTLSClientConfigFunc(logger, fs, opts...)
}
