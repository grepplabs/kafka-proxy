package proxy

import (
	"crypto/x509"
	"fmt"

	"github.com/grepplabs/kafka-proxy/config"
	"github.com/grepplabs/kafka-proxy/proxy/clientcertvalidate"
)

func tlsClientCertVerificationFunc(conf *config.Config) (func([][]byte, [][]*x509.Certificate) error, error) {
	parsedSubjects, parserErr := getParsedSubjects(conf)
	if parserErr != nil {
		return func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error { return nil }, parserErr
	}
	return func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {

		if len(parsedSubjects) == 0 {
			return nil // nothing to validate
		}

		errs := []error{}

		for _, chain := range verifiedChains {
			for _, cert := range chain {

				certificateAcceptable := false

				// as soon as any parsed subject validates, pass the request;
				for _, parsedSubject := range parsedSubjects {
					x509ValidateErr := parsedSubject.X509Validate(cert)
					if x509ValidateErr == nil {
						certificateAcceptable = true
						break
					}
					errs = append(errs, x509ValidateErr)
				}

				if certificateAcceptable {
					return nil
				}

			}
		}

		return fmt.Errorf("tls: no client certificate presented for any of the defined client subjects, errors: '%v'", errs)

	}, nil
}

func getParsedSubjects(conf *config.Config) ([]clientcertvalidate.ParsedSubject, error) {
	parsedSubjects := []clientcertvalidate.ParsedSubject{}
	for _, subject := range conf.Proxy.TLS.ClientCert.Subjects {
		parser := clientcertvalidate.NewSubjectParser(subject)
		parsedSubject, parseErr := parser.Parse()
		if parseErr != nil {
			return parsedSubjects, parseErr
		}
		parsedSubjects = append(parsedSubjects, parsedSubject)
	}
	return parsedSubjects, nil
}
