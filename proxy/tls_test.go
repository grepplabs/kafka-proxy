package proxy

import (
	"bytes"
	"crypto/x509"
	"github.com/armon/go-socks5"
	"github.com/grepplabs/kafka-proxy/config"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func TestDefaultCipherSuites(t *testing.T) {
	a := assert.New(t)

	bundle := NewCertsBundle()
	defer bundle.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()

	serverConfig, err := newTLSListenerConfig(c)
	a.Nil(err)
	// TLS_FALLBACK_SCSV is added as first
	a.Equal(len(getPreferredDefaultCiphers())+1, len(serverConfig.CipherSuites))
	a.Equal(len(defaultCurvePreferences), len(serverConfig.CurvePreferences))
}

func TestEnabledCipherSuites(t *testing.T) {
	a := assert.New(t)

	bundle := NewCertsBundle()
	defer bundle.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()
	c.Proxy.TLS.ListenerCipherSuites = []string{"ECDHE-ECDSA-AES256-GCM-SHA384", "ECDHE-RSA-AES256-GCM-SHA384"}
	c.Proxy.TLS.ListenerCurvePreferences = []string{"P521"}

	serverConfig, err := newTLSListenerConfig(c)
	a.Nil(err)
	// TLS_FALLBACK_SCSV is added as first
	a.Equal(3, len(serverConfig.CipherSuites))
	a.Equal(1, len(serverConfig.CurvePreferences))
}

func TestTLSUnknownAuthorityNoCAChainCert(t *testing.T) {
	a := assert.New(t)

	bundle := NewCertsBundle()
	defer bundle.Close()

	c := new(config.Config)
	c.Proxy.TLS.ListenerCertFile = bundle.ServerCert.Name()
	c.Proxy.TLS.ListenerKeyFile = bundle.ServerKey.Name()

	_, _, _, err := makeTLSPipe(c, nil)
	a.EqualError(err, "x509: certificate signed by unknown authority")
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
	a.EqualError(err, "x509: certificate signed by unknown authority")
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
	a.True(strings.HasPrefix(err.Error(), "proxy: SOCKS5 proxy at"))
	a.True(strings.HasSuffix(err.Error(), "rejected username/password"))
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
	c.Proxy.TLS.CAChainCertFile = bundle2.CACert.Name() // client CA

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
	c.Proxy.TLS.CAChainCertFile = bundle1.CACert.Name() // client CA

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
	c.Proxy.TLS.CAChainCertFile = bundle1.CACert.Name() // client CA

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
	c.Proxy.TLS.CAChainCertFile = bundle1.CACert.Name() // client CA

	c.Kafka.TLS.CAChainCertFile = bundle1.ServerCert.Name()
	c.Kafka.TLS.ClientCertFile = bundle2.ClientCert.Name()
	c.Kafka.TLS.ClientKeyFile = bundle2.ClientKey.Name()
	_, _, _, err := makeTLSPipe(c, nil)

	a.NotNil(err)
	a.Contains(err.Error(), "tls: failed to verify client certificate")
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
	c.Proxy.TLS.CAChainCertFile = bundle2.CACert.Name() // client CA

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
		c1.SetDeadline(time.Now().Add(2 * time.Second))
		request := bytes.NewBuffer(ping)
		_, err := io.Copy(c1, request)
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

	c2.SetDeadline(time.Now().Add(2 * time.Second))
	request := make([]byte, len(ping))
	_, err := io.ReadFull(c2, request)
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

func TestDecryptPEMKey(t *testing.T) {
	a := assert.New(t)

	var testData = []struct {
		kind             x509.PEMCipher
		password         string
		pemDataEncrypted string
		pemData          string
	}{
		{
			kind:     x509.PEMCipherAES256,
			password: "VqTNL69WeJtbFERnGD8BrRBwctsWzpTPNcMZMXtufAZruxLRkcBjDbcgBDFyzKyu",
			pemDataEncrypted: `-----BEGIN RSA PRIVATE KEY-----
Proc-Type: 4,ENCRYPTED
DEK-Info: AES-256-CBC,E4DD4077F88E0609CBE3A160FBFA3AB4

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
-----END RSA PRIVATE KEY-----`,
			pemData: `-----BEGIN RSA PRIVATE KEY-----
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
-----END RSA PRIVATE KEY-----
`,
		},
	}

	for _, data := range testData {
		decryptedKey, err := decryptPEM([]byte(data.pemDataEncrypted), data.password)
		a.Nil(err)
		a.EqualValues(decryptedKey, data.pemData)

		sameKey, err := decryptPEM([]byte(data.pemData), data.password)
		a.Nil(err)
		a.EqualValues(sameKey, data.pemData)
	}
}
