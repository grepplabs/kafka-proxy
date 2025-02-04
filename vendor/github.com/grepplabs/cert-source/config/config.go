package config

import (
	"time"
)

type TLSServerConfig struct {
	Enable      bool           `help:"Enable server-side TLS."`
	Refresh     time.Duration  `default:"0s" help:"Interval for refreshing server TLS certificates."`
	File        TLSServerFiles `embed:"" prefix:"file."`
	KeyPassword string         `help:"Optional password to decrypt RSA private key."`
}

type TLSServerFiles struct {
	Key       string `placeholder:"FILE" help:"Path to the server TLS key file."`
	Cert      string `placeholder:"FILE" help:"Path to the server TLS certificate file."`
	ClientCAs string `placeholder:"FILE" name:"client-ca" help:"Optional path to server client CA file for client verification."`
	ClientCRL string `placeholder:"FILE" name:"client-crl" help:"TLS X509 CRL signed be the client CA. If no revocation list is specified, only client CA is verified."`
}

type TLSClientConfig struct {
	Enable             bool           `help:"Enable client-side TLS."`
	Refresh            time.Duration  `default:"0s" help:"Interval for refreshing client TLS certificates."`
	InsecureSkipVerify bool           `help:"Skip TLS verification on client side."`
	File               TLSClientFiles `embed:"" prefix:"file."`
	KeyPassword        string         `help:"Optional password to decrypt RSA private key."`
	UseSystemPool      bool           `help:"Use system pool for root CAs."`
}

type TLSClientFiles struct {
	Key     string `placeholder:"FILE" help:"Optional path to client TLS key file."`
	Cert    string `placeholder:"FILE" help:"Optional path to client TLS certificate file."`
	RootCAs string `placeholder:"FILE" name:"root-ca" help:"Optional path to client root CAs for server verification."`
}
