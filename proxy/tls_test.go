package proxy

import (
	"bytes"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/armon/go-socks5"
	"github.com/grepplabs/kafka-proxy/config"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestDefaultCipherSuites(t *testing.T) {
	a := assert.New(t)

	bundle := NewCertsBundle()
	defer bundle.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Proxy.TLS.ListenerCipherSuites = []string{}
	c.Proxy.TLS.ListenerCurvePreferences = []string{}

	serverConfig, err := newTLSListenerConfig(c)
	a.Nil(err)
	a.Nil(serverConfig.CipherSuites)
	a.Nil(serverConfig.CurvePreferences)
}

func TestEnabledCipherSuitesAndCurves(t *testing.T) {
	a := assert.New(t)

	bundle := NewCertsBundle()
	defer bundle.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Proxy.TLS.ListenerCipherSuites = []string{"TLS_AES_128_GCM_SHA256", "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"}
	c.Proxy.TLS.ListenerCurvePreferences = []string{"P521"}

	serverConfig, err := newTLSListenerConfig(c)
	a.Nil(err)
	a.Equal(2, len(serverConfig.CipherSuites))
	a.Equal(1, len(serverConfig.CurvePreferences))
}

func TestAllCipherSuitesAndCurves(t *testing.T) {
	a := assert.New(t)

	bundle := NewCertsBundle()
	defer bundle.Close()

	allSupportedCipherSuites := make([]string, 0)
	for k := range supportedCiphersMap {
		allSupportedCipherSuites = append(allSupportedCipherSuites, k)
	}
	allSupportedCurvesSuites := make([]string, 0)
	for k := range supportedCurvesMap {
		allSupportedCurvesSuites = append(allSupportedCurvesSuites, k)
	}

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Proxy.TLS.ListenerCipherSuites = allSupportedCipherSuites
	c.Proxy.TLS.ListenerCurvePreferences = allSupportedCurvesSuites

	serverConfig, err := newTLSListenerConfig(c)
	a.Nil(err)
	a.Equal(len(allSupportedCipherSuites), len(serverConfig.CipherSuites))
	a.Equal(len(allSupportedCurvesSuites), len(serverConfig.CurvePreferences))
}

func TestUnsupportedCipherSuite(t *testing.T) {
	a := assert.New(t)

	bundle := NewCertsBundle()
	defer bundle.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Proxy.TLS.ListenerCipherSuites = []string{"TLS_unknown"}

	_, err := newTLSListenerConfig(c)
	a.NotNil(err)
}

func TestUnsupportedCurve(t *testing.T) {
	a := assert.New(t)

	bundle := NewCertsBundle()
	defer bundle.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Proxy.TLS.ListenerCurvePreferences = []string{"unknown"}

	_, err := newTLSListenerConfig(c)
	a.NotNil(err)
}

func TestTLSUnknownAuthorityNoCAChainCert(t *testing.T) {
	a := assert.New(t)

	bundle := NewCertsBundle()
	defer bundle.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()

	_, _, _, err := makeTLSPipe(c, nil)
	// Can be
	// - tls: failed to verify certificate: x509: “localhost” certificate is not standards compliant
	// - tls: failed to verify certificate: x509: certificate signed by unknown authority
	a.ErrorContains(err, "tls: failed to verify certificate: x509:")
}

func TestTLSUnknownAuthorityWrongCAChainCert(t *testing.T) {
	a := assert.New(t)

	bundle1 := NewCertsBundle()
	defer bundle1.Close()

	bundle2 := NewCertsBundle()
	defer bundle2.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle1.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle1.ServerKey.Name()
	// different bundle -> incorrect cert
	c.Kafka.TLS.CAChainCertFile = bundle2.ServerCert.Name()

	_, _, _, err := makeTLSPipe(c, nil)
	a.EqualError(err, "tls: failed to verify certificate: x509: certificate signed by unknown authority")
}

func TestTLSInsecureSkipVerify(t *testing.T) {
	a := assert.New(t)

	bundle := NewCertsBundle()
	defer bundle.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Kafka.TLS.InsecureSkipVerify = true

	c1, c2, stop, err := makeTLSPipe(c, nil)
	if err != nil {
		a.FailNow(err.Error())
	}
	defer stop()
	pingPong(t, c1, c2)
}

func TestTLSSelfSigned(t *testing.T) {
	a := assert.New(t)

	bundle := NewCertsBundle()
	defer bundle.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Kafka.TLS.CAChainCertFile = bundle.ServerCert.Name()

	c1, c2, stop, err := makeTLSPipe(c, nil)
	if err != nil {
		a.FailNow(err.Error())
	}
	defer stop()
	pingPong(t, c1, c2)
}

func TestTLSThroughSocks5Proxy(t *testing.T) {
	a := assert.New(t)

	bundle := NewCertsBundle()
	defer bundle.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Kafka.TLS.CAChainCertFile = bundle.ServerCert.Name()

	c1, c2, stop, err := makeTLSSocks5ProxyPipe(c, nil, "", "")
	if err != nil {
		a.FailNow(err.Error())
	}
	defer stop()
	pingPong(t, c1, c2)
}

func TestTLSThroughHttpProxy(t *testing.T) {
	a := assert.New(t)

	bundle := NewCertsBundle()
	defer bundle.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Kafka.TLS.CAChainCertFile = bundle.ServerCert.Name()

	c1, c2, stop, err := makeTLSHttpProxyPipe(c, "", "", "", "")
	if err != nil {
		a.FailNow(err.Error())
	}
	defer stop()
	pingPong(t, c1, c2)
}

func TestTLSThroughSocks5ProxyWithCredentials(t *testing.T) {
	a := assert.New(t)

	bundle := NewCertsBundle()
	defer bundle.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Kafka.TLS.CAChainCertFile = bundle.ServerCert.Name()

	authenticator := &socks5.UserPassAuthenticator{
		Credentials: testCredentials{
			username: "test-user",
			password: "test-password",
		},
	}
	c1, c2, stop, err := makeTLSSocks5ProxyPipe(c, authenticator, "test-user", "test-password")
	if err != nil {
		a.FailNow(err.Error())
	}
	defer stop()
	pingPong(t, c1, c2)
}

func TestTLSThroughHttpProxyWithCredentials(t *testing.T) {
	a := assert.New(t)

	bundle := NewCertsBundle()
	defer bundle.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Kafka.TLS.CAChainCertFile = bundle.ServerCert.Name()
	c1, c2, stop, err := makeTLSHttpProxyPipe(c, "test-user", "test-password", "test-user", "test-password")
	if err != nil {
		a.FailNow(err.Error())
	}
	defer stop()
	pingPong(t, c1, c2)
}

func TestTLSThroughSocks5ProxyWithBadCredentials(t *testing.T) {
	a := assert.New(t)

	bundle := NewCertsBundle()
	defer bundle.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Kafka.TLS.CAChainCertFile = bundle.ServerCert.Name()

	authenticator := &socks5.UserPassAuthenticator{
		Credentials: testCredentials{
			username: "test-user",
			password: "test-password",
		},
	}
	_, _, _, err := makeTLSSocks5ProxyPipe(c, authenticator, "test-user", "bad-password")
	a.NotNil(err)
	a.True(strings.HasPrefix(err.Error(), "socks connect"))
	a.True(strings.HasSuffix(err.Error(), "username/password authentication failed"))
}

func TestTLSThroughHttpProxyWithBadCredentials(t *testing.T) {
	a := assert.New(t)

	bundle := NewCertsBundle()
	defer bundle.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Kafka.TLS.CAChainCertFile = bundle.ServerCert.Name()

	_, _, _, err := makeTLSHttpProxyPipe(c, "test-user", "test-password", "test-user", "bad-password")
	a.NotNil(err)
	a.Equal(err.Error(), "connect server using proxy error, statuscode [407]")
}

func TestTLSVerifyClientCertDifferentCAs(t *testing.T) {
	a := assert.New(t)

	bundle1 := NewCertsBundle()
	defer bundle1.Close()

	bundle2 := NewCertsBundle()
	defer bundle2.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle1.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle1.ServerKey.Name()
	c.Proxy.TLS.ListenerCAChainCertFile = bundle2.CACert.Name() // client CA

	c.Kafka.TLS.CAChainCertFile = bundle1.ServerCert.Name()
	c.Kafka.TLS.ClientCertFile = bundle2.ClientCert.Name()
	c.Kafka.TLS.ClientKeyFile = bundle2.ClientKey.Name()

	c1, c2, stop, err := makeTLSPipe(c, nil)
	if err != nil {
		a.FailNow(err.Error())
	}
	defer stop()
	pingPong(t, c1, c2)
}

func TestTLSVerifyClientCertSameCAs(t *testing.T) {
	a := assert.New(t)

	bundle1 := NewCertsBundle()
	defer bundle1.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle1.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle1.ServerKey.Name()
	c.Proxy.TLS.ListenerCAChainCertFile = bundle1.CACert.Name() // client CA

	c.Kafka.TLS.CAChainCertFile = bundle1.ServerCert.Name()
	c.Kafka.TLS.ClientCertFile = bundle1.ClientCert.Name()
	c.Kafka.TLS.ClientKeyFile = bundle1.ClientKey.Name()

	c1, c2, stop, err := makeTLSPipe(c, nil)
	if err != nil {
		a.FailNow(err.Error())
	}
	defer stop()
	pingPong(t, c1, c2)
}

func TestTLSMissingClientCert(t *testing.T) {
	a := assert.New(t)

	bundle1 := NewCertsBundle()
	defer bundle1.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle1.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle1.ServerKey.Name()
	c.Proxy.TLS.ListenerCAChainCertFile = bundle1.CACert.Name() // client CA

	c.Kafka.TLS.CAChainCertFile = bundle1.ServerCert.Name()

	_, _, _, err := makeTLSPipe(c, nil)
	a.NotNil(err)
	a.Contains(err.Error(), "tls: client didn't provide a certificate")
}

func TestTLSBadClientCert(t *testing.T) {
	a := assert.New(t)

	bundle1 := NewCertsBundle()
	defer bundle1.Close()

	bundle2 := NewCertsBundle()
	defer bundle2.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle1.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle1.ServerKey.Name()
	c.Proxy.TLS.ListenerCAChainCertFile = bundle1.CACert.Name() // client CA

	c.Kafka.TLS.CAChainCertFile = bundle1.ServerCert.Name()
	c.Kafka.TLS.ClientCertFile = bundle2.ClientCert.Name()
	c.Kafka.TLS.ClientKeyFile = bundle2.ClientKey.Name()
	_, _, _, err := makeTLSPipe(c, nil)

	a.NotNil(err)
	a.Contains(err.Error(), "tls: failed to verify certificate: x509: certificate signed by unknown authority")
}

func TestTLSVerifySameClientCert(t *testing.T) {

	sameCertToCompare := true
	differentCertToCompare := false

	bundle1 := NewCertsBundle()
	defer bundle1.Close()

	bundle2 := NewCertsBundle()
	defer bundle2.Close()

	t.Run("SameClientCertDisabledWithSameClientCerts", func(t *testing.T) {
		c, clientCertFileToCheck := configWithCertToCompare(bundle1, bundle2, sameCertToCompare)
		c.Kafka.TLS.SameClientCertEnable = false
		successfulPingPong(t, c, clientCertFileToCheck)
	})

	t.Run("SameClientCertDisabledWithDifferentClientCerts", func(t *testing.T) {
		c, clientCertFileToCheck := configWithCertToCompare(bundle1, bundle2, differentCertToCompare)
		c.Kafka.TLS.SameClientCertEnable = false
		successfulPingPong(t, c, clientCertFileToCheck)
	})

	t.Run("SameClientCertEnabledWithSameClientCerts", func(t *testing.T) {
		c, clientCertFileToCheck := configWithCertToCompare(bundle1, bundle2, sameCertToCompare)
		c.Kafka.TLS.SameClientCertEnable = true
		successfulPingPong(t, c, clientCertFileToCheck)
	})

	t.Run("SameClientCertEnabledWithDifferentClientCerts", func(t *testing.T) {
		c, clientCertFileToCheck := configWithCertToCompare(bundle1, bundle2, differentCertToCompare)
		c.Kafka.TLS.SameClientCertEnable = true
		pipelineSetupFailure(t, c, clientCertFileToCheck, "Client cert sent by proxy client does not match brokers client cert (tls-client-cert-file)")
	})
}

func configWithCertToCompare(bundle1 *CertsBundle, bundle2 *CertsBundle, sameCertToCompare bool) (*config.Config, string) {
	c := new(config.Config)

	c.Proxy.TLS.ListenerCertFile = bundle1.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle1.ServerKey.Name()
	c.Proxy.TLS.ListenerCAChainCertFile = bundle2.CACert.Name() // client CA

	c.Kafka.TLS.CAChainCertFile = bundle1.ServerCert.Name()
	c.Kafka.TLS.ClientCertFile = bundle2.ClientCert.Name()
	c.Kafka.TLS.ClientKeyFile = bundle2.ClientKey.Name() //client cert

	if sameCertToCompare {
		return c, bundle2.ClientCert.Name()
	}

	return c, bundle1.ClientCert.Name()
}

func successfulPingPong(t *testing.T, conf *config.Config, clientCertFileToCheck string) {
	a := assert.New(t)

	clientCertToCheck, _ := parseCertificate(clientCertFileToCheck)

	c1, c2, stop, err := makeTLSPipe(conf, clientCertToCheck)
	if err != nil {
		a.FailNow(err.Error())
	}
	defer stop()
	pingPong(t, c1, c2)
}

func pipelineSetupFailure(t *testing.T, conf *config.Config, clientCertFileToCheck string, expectedErrMsg string) {
	a := assert.New(t)

	expectedClientCert, _ := parseCertificate(clientCertFileToCheck)

	_, _, _, err := makeTLSPipe(conf, expectedClientCert)

	a.NotNil(err)
	a.Equal(err.Error(), expectedErrMsg)
}

func pingPong(t *testing.T, c1, c2 net.Conn) {
	a := assert.New(t)

	ping := []byte("ping")
	pong := []byte("pong")

	clientResult := make(chan error, 1)
	go func() {
		// send "ping"
		err := c1.SetDeadline(time.Now().Add(2 * time.Second))
		a.Nil(err)
		request := bytes.NewBuffer(ping)
		_, err = io.Copy(c1, request)
		if err != nil {
			clientResult <- err
			return
		}
		response := make([]byte, len(pong))
		_, err = io.ReadFull(c1, response)
		if err != nil {
			clientResult <- err
			return
		}
		if "pong" != string(response) {
			clientResult <- errors.New("pong expected")
			return
		}
		clientResult <- nil
	}()

	err := c2.SetDeadline(time.Now().Add(2 * time.Second))
	a.Nil(err)
	request := make([]byte, len(ping))
	_, err = io.ReadFull(c2, request)
	a.Nil(err)

	a.Equal("ping", string(request))

	response := bytes.NewBuffer(pong)
	_, err = io.Copy(c2, response)
	a.Nil(err)

	cerr := <-clientResult
	a.Nil(cerr)
}

type CertsBundle struct {
	dirName string

	CACert     *os.File
	CAKey      *os.File
	ServerCert *os.File
	ServerKey  *os.File
	ClientCert *os.File
	ClientKey  *os.File
}
