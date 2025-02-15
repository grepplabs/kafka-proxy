package proxy

import (
	"crypto/x509/pkix"
	"fmt"
	"strings"
	"testing"

	"github.com/grepplabs/kafka-proxy/config"
	"github.com/stretchr/testify/assert"
)

func TestValidEnabledClientCertSubjectValidate(t *testing.T) {
	a := assert.New(t)
	testSubject := pkix.Name{
		CommonName:         "integration-test",
		Country:            []string{"DE"},
		Province:           []string{"NRW"},
		Locality:           []string{"test-file"},
		Organization:       []string{"integration-test"},
		OrganizationalUnit: []string{"valid-OrganizationalUnit"},
	}
	bundle := NewCertsBundleWithSubject(testSubject)
	defer bundle.Close()
	c := new(config.Config)

	c.Proxy.TLS.ClientCert.Subjects = []string{
		fmt.Sprintf("s:/CN=[%s]/C=[%s]/S=[%s]/L=[%s]/O=[%s]/OU=[%s]",
			testSubject.CommonName,
			strings.Join(testSubject.Country, ","),
			strings.Join(testSubject.Province, ","),
			strings.Join(testSubject.Locality, ","),
			strings.Join(testSubject.Organization, ","),
			strings.Join(testSubject.OrganizationalUnit, ",")),
	}

	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Proxy.TLS.ListenerCAChainCertFile = bundle.CACert.Name()

	c.Kafka.TLS.CAChainCertFile = bundle.CACert.Name()
	c.Kafka.TLS.ClientCertFile = bundle.ClientCert.Name()
	c.Kafka.TLS.ClientKeyFile = bundle.ClientKey.Name()

	_, _, _, err := makeTLSPipe(c, nil)

	a.Nil(err)
}

func TestInvalidEnabledClientCertSubjectValidate(t *testing.T) {
	a := assert.New(t)
	testSubject := pkix.Name{
		CommonName:         "integration-test",
		Country:            []string{"DE"},
		Province:           []string{"NRW"},
		Locality:           []string{"test-file"},
		Organization:       []string{"integration-test"},
		OrganizationalUnit: []string{"invalid-OrganizationalUnit"},
	}
	bundle := NewCertsBundleWithSubject(testSubject)
	defer bundle.Close()
	c := new(config.Config)
	c.Proxy.TLS.ClientCert.Subjects = []string{
		fmt.Sprintf("s:/CN=[%s]/C=[%s]/S=[%s]/L=[%s]/O=[%s]/OU=[%s]",
			testSubject.CommonName,
			strings.Join(testSubject.Country, ","),
			strings.Join(testSubject.Province, ","),
			strings.Join(testSubject.Locality, ","),
			strings.Join(testSubject.Organization, ","),
			strings.Join([]string{"expected-OrganizationalUnit"}, ",")),
	}
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Proxy.TLS.ListenerCAChainCertFile = bundle.CACert.Name()

	c.Kafka.TLS.CAChainCertFile = bundle.CACert.Name()
	c.Kafka.TLS.ClientCertFile = bundle.ClientCert.Name()
	c.Kafka.TLS.ClientKeyFile = bundle.ClientKey.Name()

	_, _, _, err := makeTLSPipe(c, nil)

	a.NotNil(err)
	a.Contains(err.Error(), "tls: no client certificate presented for any of the defined client subjects")
}

func TestValidEnabledClientCertSubjectMayContainNotRequiredValues(t *testing.T) {
	a := assert.New(t)
	testSubject := pkix.Name{
		CommonName:         "integration-test",
		Country:            []string{"DE"},
		Province:           []string{"NRW"},
		Locality:           []string{"locality-not-validated"},
		Organization:       []string{"integration-test"},
		OrganizationalUnit: []string{"invalid-OrganizationalUnit"},
	}
	bundle := NewCertsBundleWithSubject(testSubject)
	defer bundle.Close()
	c := new(config.Config)
	c.Proxy.TLS.ClientCert.Subjects = []string{
		fmt.Sprintf("s:/CN=[%s]/C=[%s]/S=[%s]/O=[%s]/OU=[%s]",
			testSubject.CommonName,
			strings.Join(testSubject.Country, ","),
			strings.Join(testSubject.Province, ","),
			strings.Join(testSubject.Organization, ","),
			strings.Join(testSubject.OrganizationalUnit, ",")),
	}
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Proxy.TLS.ListenerCAChainCertFile = bundle.CACert.Name()

	c.Kafka.TLS.CAChainCertFile = bundle.CACert.Name()
	c.Kafka.TLS.ClientCertFile = bundle.ClientCert.Name()
	c.Kafka.TLS.ClientKeyFile = bundle.ClientKey.Name()

	_, _, _, err := makeTLSPipe(c, nil)

	a.Nil(err)
}

func TestValidEnabledClientCertSubjectMayContainValuesInDifferentOrder(t *testing.T) {
	a := assert.New(t)
	testSubject := pkix.Name{
		CommonName:         "integration-test",
		Country:            []string{"DE", "PL"},
		Province:           []string{"NRW"},
		Organization:       []string{"integration-test"},
		OrganizationalUnit: []string{"invalid-OrganizationalUnit"},
	}
	bundle := NewCertsBundleWithSubject(testSubject)
	defer bundle.Close()
	c := new(config.Config)
	c.Proxy.TLS.ClientCert.Subjects = []string{
		fmt.Sprintf("s:/C=[%s]",
			strings.Join([]string{"PL", "DE"}, ",")),
	}
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Proxy.TLS.ListenerCAChainCertFile = bundle.CACert.Name()

	c.Kafka.TLS.CAChainCertFile = bundle.CACert.Name()
	c.Kafka.TLS.ClientCertFile = bundle.ClientCert.Name()
	c.Kafka.TLS.ClientKeyFile = bundle.ClientKey.Name()

	_, _, _, err := makeTLSPipe(c, nil)

	a.Nil(err)
}

func TestClientCertMultipleSubjects(t *testing.T) {
	a := assert.New(t)
	testSubject := pkix.Name{
		CommonName:         "integration-test",
		Country:            []string{"DE"},
		Province:           []string{"NRW"},
		Locality:           []string{"test-file"},
		Organization:       []string{"integration-test"},
		OrganizationalUnit: []string{"valid-OrganizationalUnit"},
	}
	bundle := NewCertsBundleWithSubject(testSubject)
	defer bundle.Close()
	c := new(config.Config)

	c.Proxy.TLS.ClientCert.Subjects = []string{
		fmt.Sprintf("s:/CN=[%s]/C=[%s]/S=[%s]/L=[%s]/O=[%s]/OU=[%s]",
			"first one does not match",
			strings.Join(testSubject.Country, ","),
			strings.Join(testSubject.Province, ","),
			strings.Join(testSubject.Locality, ","),
			strings.Join(testSubject.Organization, ","),
			strings.Join(testSubject.OrganizationalUnit, ",")),
		fmt.Sprintf("s:/CN=[%s]/C=[%s]/S=[%s]/L=[%s]/O=[%s]/OU=[%s]",
			testSubject.CommonName,
			strings.Join(testSubject.Country, ","),
			strings.Join(testSubject.Province, ","),
			strings.Join(testSubject.Locality, ","),
			strings.Join(testSubject.Organization, ","),
			strings.Join(testSubject.OrganizationalUnit, ",")),
	}

	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Proxy.TLS.ListenerCAChainCertFile = bundle.CACert.Name()

	c.Kafka.TLS.CAChainCertFile = bundle.CACert.Name()
	c.Kafka.TLS.ClientCertFile = bundle.ClientCert.Name()
	c.Kafka.TLS.ClientKeyFile = bundle.ClientKey.Name()

	_, _, _, err := makeTLSPipe(c, nil)

	a.Nil(err)
}

func TestClientCertMultipleSubjectsPatterns(t *testing.T) {
	a := assert.New(t)
	testSubject := pkix.Name{
		CommonName:         "integration-test-123",
		Country:            []string{"DE"},
		Province:           []string{"NRW"},
		Locality:           []string{"test-file"},
		Organization:       []string{"integration-test"},
		OrganizationalUnit: []string{"valid-OrganizationalUnit"},
	}
	bundle := NewCertsBundleWithSubject(testSubject)
	defer bundle.Close()
	c := new(config.Config)

	c.Proxy.TLS.ClientCert.Subjects = []string{
		fmt.Sprintf("r:/CN=[%s]/C=[%s]/S=[%s]/L=[%s]/O=[%s]/OU=[%s]",
			"first one does not match",
			strings.Join(testSubject.Country, ","),
			strings.Join(testSubject.Province, ","),
			strings.Join(testSubject.Locality, ","),
			strings.Join(testSubject.Organization, ","),
			strings.Join(testSubject.OrganizationalUnit, ",")),
		fmt.Sprintf("r:/CN=[%s]/C=[%s]/S=[%s]/L=[%s]/O=[%s]/OU=[%s]",
			"integration-test-\\d{1,}",
			strings.Join(testSubject.Country, ","),
			strings.Join(testSubject.Province, ","),
			strings.Join(testSubject.Locality, ","),
			strings.Join(testSubject.Organization, ","),
			strings.Join(testSubject.OrganizationalUnit, ",")),
	}

	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Proxy.TLS.ListenerCAChainCertFile = bundle.CACert.Name()

	c.Kafka.TLS.CAChainCertFile = bundle.CACert.Name()
	c.Kafka.TLS.ClientCertFile = bundle.ClientCert.Name()
	c.Kafka.TLS.ClientKeyFile = bundle.ClientKey.Name()

	_, _, _, err := makeTLSPipe(c, nil)

	a.Nil(err)
}

func TestClientCertMultiplePatternMatchingFields(t *testing.T) {
	a := assert.New(t)
	testSubject := pkix.Name{
		CommonName:         "integration-test-123",
		Country:            []string{"DE"},
		Province:           []string{"NRW"},
		Locality:           []string{"test-file"},
		Organization:       []string{"integration-test"},
		OrganizationalUnit: []string{"hello.world.123domain"},
	}
	bundle := NewCertsBundleWithSubject(testSubject)
	defer bundle.Close()
	c := new(config.Config)

	c.Proxy.TLS.ClientCert.Subjects = []string{
		fmt.Sprintf("r:/CN=[%s]/C=[%s]/S=[%s]/L=[%s]/O=[%s]/OU=[%s]",
			"integration-test-\\d{1,}",
			strings.Join(testSubject.Country, ","),
			strings.Join(testSubject.Province, ","),
			strings.Join(testSubject.Locality, ","),
			strings.Join(testSubject.Organization, ","),
			strings.Join([]string{"^hello\\.world\\.\\d{1,3}.*$"}, ",")),
	}

	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Proxy.TLS.ListenerCAChainCertFile = bundle.CACert.Name()

	c.Kafka.TLS.CAChainCertFile = bundle.CACert.Name()
	c.Kafka.TLS.ClientCertFile = bundle.ClientCert.Name()
	c.Kafka.TLS.ClientKeyFile = bundle.ClientKey.Name()

	_, _, _, err := makeTLSPipe(c, nil)

	a.Nil(err)
}

func TestClientCertMultiplePatternNotMatchingFields(t *testing.T) {
	a := assert.New(t)
	testSubject := pkix.Name{
		CommonName:         "integration-test-123",
		Country:            []string{"DE"},
		Province:           []string{"NRW"},
		Locality:           []string{"test-file"},
		Organization:       []string{"integration-test"},
		OrganizationalUnit: []string{"hello.world.123domain"},
	}
	bundle := NewCertsBundleWithSubject(testSubject)
	defer bundle.Close()
	c := new(config.Config)

	c.Proxy.TLS.ClientCert.Subjects = []string{
		fmt.Sprintf("r:/CN=[%s]/C=[%s]/S=[%s]/L=[%s]/O=[%s]/OU=[%s]",
			"integration-test-\\d{1,}",
			strings.Join(testSubject.Country, ","),
			strings.Join(testSubject.Province, ","),
			strings.Join(testSubject.Locality, ","),
			strings.Join(testSubject.Organization, ","),
			strings.Join([]string{"^hello\\.world\\.\\d{6}domain$"}, ",")),
	}

	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Proxy.TLS.ListenerCAChainCertFile = bundle.CACert.Name()

	c.Kafka.TLS.CAChainCertFile = bundle.CACert.Name()
	c.Kafka.TLS.ClientCertFile = bundle.ClientCert.Name()
	c.Kafka.TLS.ClientKeyFile = bundle.ClientKey.Name()

	_, _, _, err := makeTLSPipe(c, nil)

	a.NotNil(err)
}

func TestClientCertMultiplePatternMatchingFieldsOrderDoesNotMatter(t *testing.T) {
	a := assert.New(t)
	testSubject := pkix.Name{
		CommonName:         "integration-test-123",
		Country:            []string{"DE"},
		Province:           []string{"NRW"},
		Locality:           []string{"test-file"},
		Organization:       []string{"integration-test"},
		OrganizationalUnit: []string{"zero.world.123domain", "zulu-important"},
	}
	bundle := NewCertsBundleWithSubject(testSubject)
	defer bundle.Close()
	c := new(config.Config)

	c.Proxy.TLS.ClientCert.Subjects = []string{
		fmt.Sprintf("r:/CN=[%s]/C=[%s]/S=[%s]/L=[%s]/O=[%s]/OU=[%s]",
			"integration-test-\\d{1,}",
			strings.Join(testSubject.Country, ","),
			strings.Join(testSubject.Province, ","),
			strings.Join(testSubject.Locality, ","),
			strings.Join(testSubject.Organization, ","),
			strings.Join([]string{"^zulu.*$", "^zero\\.world\\.\\d{1,3}.*$"}, ",")),
	}

	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Proxy.TLS.ListenerCAChainCertFile = bundle.CACert.Name()

	c.Kafka.TLS.CAChainCertFile = bundle.CACert.Name()
	c.Kafka.TLS.ClientCertFile = bundle.ClientCert.Name()
	c.Kafka.TLS.ClientKeyFile = bundle.ClientKey.Name()

	_, _, _, err := makeTLSPipe(c, nil)

	a.Nil(err)
}
