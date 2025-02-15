package tlsclient

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"

	"github.com/grepplabs/cert-source/tls/client/source"
)

type RoundTripper struct {
	transport *http.Transport
}

type RoundTripperOption func(*RoundTripper)

func WithClientTLSConfig(tlsClientConfig *tls.Config) RoundTripperOption {
	return func(rt *RoundTripper) {
		rt.transport.TLSClientConfig = tlsClientConfig
	}
}

func WithClientCertsStore(source *source.ClientCertsStore) RoundTripperOption {
	return func(rt *RoundTripper) {
		cs := source.LoadClientCerts()
		if rt.transport.TLSClientConfig == nil {
			rt.transport.TLSClientConfig = &tls.Config{}
		}
		rt.transport.TLSClientConfig.RootCAs = cs.RootCAs
		rt.transport.TLSClientConfig.InsecureSkipVerify = cs.InsecureSkipVerify
		rt.transport.TLSClientConfig.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return source.LoadClientCerts().Certificate, nil
		}
	}
}

func WithSystemRootCA(cert *x509.Certificate) RoundTripperOption {
	certPool, err := x509.SystemCertPool()
	if err != nil {
		certPool = x509.NewCertPool()
	}
	certPool.AddCert(cert)
	return WithRootCAs(certPool)
}

func WithRootCA(cert *x509.Certificate) RoundTripperOption {
	certPool := x509.NewCertPool()
	certPool.AddCert(cert)
	return WithRootCAs(certPool)
}

func WithRootCAs(rootCAs *x509.CertPool) RoundTripperOption {
	return func(rt *RoundTripper) {
		if rt.transport.TLSClientConfig == nil {
			rt.transport.TLSClientConfig = &tls.Config{}
		}
		rt.transport.TLSClientConfig.RootCAs = rootCAs
	}
}

func WithClientTLSSkipVerify(skipVerify bool) RoundTripperOption {
	return func(rt *RoundTripper) {
		if rt.transport.TLSClientConfig == nil {
			rt.transport.TLSClientConfig = &tls.Config{}
		}
		rt.transport.TLSClientConfig.InsecureSkipVerify = skipVerify
	}
}

func WithClientCertificate(clientCert *tls.Certificate) RoundTripperOption {
	return func(rt *RoundTripper) {
		if rt.transport.TLSClientConfig == nil {
			rt.transport.TLSClientConfig = &tls.Config{}
		}
		rt.transport.TLSClientConfig.GetClientCertificate = func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return clientCert, nil
		}
	}
}

func WithClientNextProtos(nextProto []string) RoundTripperOption {
	return func(rt *RoundTripper) {
		if rt.transport.TLSClientConfig == nil {
			rt.transport.TLSClientConfig = &tls.Config{}
		}
		rt.transport.TLSClientConfig.NextProtos = nextProto
	}
}

func NewRoundTripper(transport *http.Transport, options ...RoundTripperOption) *RoundTripper {
	rt := &RoundTripper{
		transport: transport,
	}
	for _, option := range options {
		option(rt)
	}
	return rt
}

func NewDefaultRoundTripper(options ...RoundTripperOption) *RoundTripper {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	return NewRoundTripper(transport, options...)
}

func (p *RoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return p.transport.RoundTrip(req)
}
