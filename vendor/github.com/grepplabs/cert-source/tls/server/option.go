package tlsserver

import (
	"crypto/tls"
	"crypto/x509"
)

type TLSServerConfigOption func(*tls.Config)

func WithTLSServerNextProtos(nextProto []string) TLSServerConfigOption {
	return func(c *tls.Config) {
		c.NextProtos = nextProto
	}
}

func WithTLSServerCurvePreferences(curvePreferences []tls.CurveID) TLSServerConfigOption {
	return func(c *tls.Config) {
		if len(curvePreferences) != 0 {
			c.CurvePreferences = curvePreferences
		} else {
			c.CurvePreferences = nil
		}
	}
}

func WithTLSServerCipherSuites(cipherSuites []uint16) TLSServerConfigOption {
	return func(c *tls.Config) {
		if len(cipherSuites) != 0 {
			c.CipherSuites = cipherSuites
		} else {
			c.CipherSuites = nil
		}
	}
}

type VerifyPeerCertificateFunc func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error

// WithTLSServerVerifyPeerCertificate sets or chains a custom VerifyPeerCertificate function on a *tls.Config.
// If a nil function is provided, it unsets the certificate verification function (including the standard verification).
// If an existing verification function is present, the new function is chained so that it is invoked only if the existing one succeeds.
func WithTLSServerVerifyPeerCertificate(verifyFunc VerifyPeerCertificateFunc) TLSServerConfigOption {
	return func(c *tls.Config) {
		if verifyFunc == nil {
			c.VerifyPeerCertificate = nil
			return
		}
		prevFunc := c.VerifyPeerCertificate
		if prevFunc == nil {
			c.VerifyPeerCertificate = verifyFunc
		} else {
			c.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
				if err := prevFunc(rawCerts, verifiedChains); err != nil {
					return err
				}
				return verifyFunc(rawCerts, verifiedChains)
			}
		}
	}
}
