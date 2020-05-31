package proxy

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"github.com/armon/go-socks5"
	"github.com/elazarl/goproxy"
	"github.com/elazarl/goproxy/ext/auth"
	"github.com/grepplabs/kafka-proxy/config"
	"github.com/pkg/errors"
	"golang.org/x/net/proxy"
	"io/ioutil"
	"math/big"
	"net"
	"net/http"
	"os"
	"time"
)

type testAcceptResult struct {
	conn net.Conn
	err  error
}

func localPipe(listener net.Listener, dialer proxy.Dialer, timeout time.Duration, expectedClientCert *x509.Certificate) (net.Conn, net.Conn, error) {
	acceptResultChannel := make(chan testAcceptResult, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptResultChannel <- testAcceptResult{
				conn: conn,
				err:  err,
			}
			return
		}

		if expectedClientCert != nil {
			err = handshakeAsTLSAndValidateClientCert(conn, expectedClientCert, timeout)

			if err != nil {
				acceptResultChannel <- testAcceptResult{
					conn: conn,
					err:  err,
				}
				return
			}
		}

		// will force handshake completion
		buf := make([]byte, 0)
		_, err = conn.Read(buf)

		tlsconn, ok := conn.(*tls.Conn)
		if ok {
			state := tlsconn.ConnectionState()
			for _, v := range state.PeerCertificates {
				_ = v
				// fmt.Println(x509.MarshalPKIXPublicKey(v.PublicKey))
			}
		}
		acceptResultChannel <- testAcceptResult{
			conn: conn,
			err:  err,
		}
	}()
	addr := listener.Addr()
	c1, err := dialer.Dial(addr.Network(), addr.String())
	if err != nil {
		return nil, nil, err
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	var acceptResult testAcceptResult
	select {
	case acceptResult = <-acceptResultChannel:
	case <-timer.C:
		return nil, nil, errors.New("accept request timeout")
	}
	return c1, acceptResult.conn, acceptResult.err
}

func makeTLSPipe(conf *config.Config, expectedClientCert *x509.Certificate) (net.Conn, net.Conn, func(), error) {
	stop := func() {}

	serverConfig, err := newTLSListenerConfig(conf)
	if err != nil {
		return nil, nil, stop, err
	}
	clientConfig, err := newTLSClientConfig(conf)
	if err != nil {
		return nil, nil, stop, err
	}
	var clientCertToCheck *x509.Certificate = nil
	if conf.Kafka.TLS.SameClientCertEnable {
		clientCertToCheck = expectedClientCert
	}
	tlsListener, err := tls.Listen("tcp", "127.0.0.1:0", serverConfig)
	if err != nil {
		return nil, nil, stop, err
	}
	tlsDialer := tlsDialer{
		timeout: 3 * time.Second,
		rawDialer: directDialer{
			dialTimeout: 3 * time.Second,
			keepAlive:   60 * time.Second,
		},
		config: clientConfig,
	}
	c1, c2, err := localPipe(tlsListener, tlsDialer, 4*time.Second, clientCertToCheck)
	stop = func() {
		if c1 != nil {
			c1.Close()
		}
		if c1 != nil {
			c2.Close()
		}
		_ = tlsListener.Close()
	}
	return c1, c2, stop, err
}

type testCredentials struct {
	username, password string
}

func (s testCredentials) Valid(username, password string) bool {
	return s.username == username && s.password == password
}

func makeTLSSocks5ProxyPipe(conf *config.Config, authenticator socks5.Authenticator, username, password string) (net.Conn, net.Conn, func(), error) {
	stop := func() {}
	socks5Conf := &socks5.Config{}
	if authenticator != nil {
		socks5Conf.AuthMethods = []socks5.Authenticator{authenticator}
	}
	clientConfig, err := newTLSClientConfig(conf)
	if err != nil {
		return nil, nil, stop, err
	}
	serverConfig, err := newTLSListenerConfig(conf)
	if err != nil {
		return nil, nil, stop, err
	}
	server, err := socks5.New(socks5Conf)
	if err != nil {
		return nil, nil, stop, err
	}
	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, nil, stop, err
	}
	stop = func() {
		_ = proxyListener.Close()
	}
	socksDialer := socks5Dialer{
		directDialer: directDialer{
			dialTimeout: 2 * time.Second,
			keepAlive:   60 * time.Second,
		},
		proxyNetwork: proxyListener.Addr().Network(),
		proxyAddr:    proxyListener.Addr().String(),
		username:     username,
		password:     password,
	}

	tlsDialer := tlsDialer{
		timeout:   3 * time.Second,
		rawDialer: socksDialer,
		config:    clientConfig,
	}

	tlsListener, err := tls.Listen("tcp", "127.0.0.1:0", serverConfig)
	if err != nil {
		return nil, nil, stop, err
	}

	go func() {
		if proxyConn, proxyErr := proxyListener.Accept(); proxyErr == nil {
			_ = server.ServeConn(proxyConn)
		}
	}()

	c1, c2, err := localPipe(tlsListener, tlsDialer, 4*time.Second, nil)
	stop = func() {
		if c1 != nil {
			c1.Close()
		}
		if c1 != nil {
			c2.Close()
		}
		_ = tlsListener.Close()
		_ = proxyListener.Close()
	}
	return c1, c2, stop, err
}

func makeTLSHttpProxyPipe(conf *config.Config, proxyusername, proxypassword string, username, password string) (net.Conn, net.Conn, func(), error) {
	stop := func() {}
	server := goproxy.NewProxyHttpServer()
	var err error
	if proxyusername != "" && proxypassword != "" {
		server.OnRequest().HandleConnect(auth.BasicConnect("", func(user, passwd string) bool {
			return user == proxyusername && passwd == proxypassword
		}))
	}
	if err != nil {
		return nil, nil, stop, err
	}
	clientConfig, err := newTLSClientConfig(conf)
	if err != nil {
		return nil, nil, stop, err
	}
	serverConfig, err := newTLSListenerConfig(conf)
	if err != nil {
		return nil, nil, stop, err
	}
	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, nil, stop, err
	}
	stop = func() {
		_ = proxyListener.Close()
	}
	httpProxy := &httpProxy{
		forwardDialer: directDialer{
			dialTimeout: 2 * time.Second,
			keepAlive:   60 * time.Second,
		},
		network:  proxyListener.Addr().Network(),
		hostPort: proxyListener.Addr().String(),
		username: username,
		password: password,
	}

	tlsDialer := tlsDialer{
		timeout:   3 * time.Second,
		rawDialer: httpProxy,
		config:    clientConfig,
	}

	tlsListener, err := tls.Listen("tcp", "127.0.0.1:0", serverConfig)
	if err != nil {
		return nil, nil, stop, err
	}

	go func() {
		_ = http.Serve(proxyListener, server)
	}()

	c1, c2, err := localPipe(tlsListener, tlsDialer, 4*time.Second, nil)
	stop = func() {
		if c1 != nil {
			c1.Close()
		}
		if c1 != nil {
			c2.Close()
		}
		_ = tlsListener.Close()
		_ = proxyListener.Close()
	}
	return c1, c2, stop, err
}

func makePipe() (net.Conn, net.Conn, func(), error) {
	stop := func() {}

	dialer := directDialer{
		dialTimeout: 2 * time.Second,
		keepAlive:   60 * time.Second,
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, nil, stop, err
	}

	c1, c2, err := localPipe(listener, dialer, 4*time.Second, nil)
	stop = func() {
		if c1 != nil {
			c1.Close()
		}
		if c1 != nil {
			c2.Close()
		}
		_ = listener.Close()
	}
	return c1, c2, stop, err
}

func makeSocks5ProxyPipe() (net.Conn, net.Conn, func(), error) {
	stop := func() {}
	server, err := socks5.New(&socks5.Config{})
	if err != nil {
		return nil, nil, stop, err
	}
	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, nil, stop, err
	}
	stop = func() {
		_ = proxyListener.Close()
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, nil, stop, err
	}
	socksDialer := socks5Dialer{
		directDialer: directDialer{
			dialTimeout: 2 * time.Second,
			keepAlive:   60 * time.Second,
		},
		proxyNetwork: proxyListener.Addr().Network(),
		proxyAddr:    proxyListener.Addr().String(),
	}
	go func() {
		if proxyConn, proxyErr := proxyListener.Accept(); proxyErr == nil {
			_ = server.ServeConn(proxyConn)
		}
	}()

	c1, c2, err := localPipe(listener, socksDialer, 4*time.Second, nil)
	stop = func() {
		if c1 != nil {
			c1.Close()
		}
		if c1 != nil {
			c2.Close()
		}
		_ = listener.Close()
		_ = proxyListener.Close()
	}
	return c1, c2, stop, err
}

func makeHttpProxyPipe() (net.Conn, net.Conn, func(), error) {
	stop := func() {}
	server := goproxy.NewProxyHttpServer()
	//server.Verbose = true

	proxyListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, nil, stop, err
	}
	stop = func() {
		_ = proxyListener.Close()
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, nil, stop, err
	}
	httpProxyDialer := httpProxy{
		forwardDialer: directDialer{
			dialTimeout: 2 * time.Second,
			keepAlive:   60 * time.Second,
		},
		network:  proxyListener.Addr().Network(),
		hostPort: proxyListener.Addr().String(),
	}

	go func() {
		_ = http.Serve(proxyListener, server)
	}()

	c1, c2, err := localPipe(listener, httpProxyDialer.forwardDialer, 4*time.Second, nil)
	stop = func() {
		if c1 != nil {
			c1.Close()
		}
		if c1 != nil {
			c2.Close()
		}
		_ = listener.Close()
		_ = proxyListener.Close()
	}
	return c1, c2, stop, err
}

func generateCert(catls *tls.Certificate, certFile *os.File, keyFile *os.File) error {
	// Prepare certificate
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization:  []string{"ORGANIZATION_NAME"},
			Country:       []string{"COUNTRY_CODE"},
			Province:      []string{"PROVINCE"},
			Locality:      []string{"CITY"},
			StreetAddress: []string{"ADDRESS"},
			PostalCode:    []string{"POSTAL_CODE"},
			CommonName:    "localhost",
		},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		SubjectKeyId: []byte{1, 2, 3, 4, 6},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IP([]byte{127, 0, 0, 1})},
	}
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	pub := &priv.PublicKey

	// tls cert -> x509 cert
	ca, err := x509.ParseCertificate(catls.Certificate[0])
	if err != nil {
		return err
	}

	// Sign the certificate
	cert_b, err := x509.CreateCertificate(rand.Reader, cert, ca, pub, catls.PrivateKey)
	if err != nil {
		return err
	}
	// Public key
	err = pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: cert_b})
	if err != nil {
		return err
	}
	err = certFile.Sync()
	if err != nil {
		return err
	}
	// Private key
	err = pem.Encode(keyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	err = keyFile.Sync()
	if err != nil {
		return err
	}
	return nil
}

func generateCA(certFile *os.File, keyFile *os.File) (*tls.Certificate, error) {
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1653),
		Subject: pkix.Name{
			Organization:  []string{"ORGANIZATION_NAME"},
			Country:       []string{"COUNTRY_CODE"},
			Province:      []string{"PROVINCE"},
			Locality:      []string{"CITY"},
			StreetAddress: []string{"ADDRESS"},
			PostalCode:    []string{"POSTAL_CODE"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}

	pub := &priv.PublicKey
	ca_b, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, priv)
	if err != nil {
		return nil, err
	}

	// Public key
	err = pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: ca_b})
	if err != nil {
		return nil, err
	}
	err = certFile.Sync()
	if err != nil {
		return nil, err
	}
	// Private key
	err = pem.Encode(keyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	if err != nil {
		return nil, err
	}
	err = keyFile.Sync()
	if err != nil {
		return nil, err
	}
	// Load CA
	catls, err := tls.LoadX509KeyPair(certFile.Name(), keyFile.Name())
	if err != nil {
		return nil, err
	}

	ca, err = x509.ParseCertificate(catls.Certificate[0])
	if err != nil {
		return nil, err
	}
	return &catls, nil
}

func NewCertsBundle() *CertsBundle {
	bundle := &CertsBundle{}
	dirName, err := ioutil.TempDir("", "tls-test")
	if err != nil {
		panic(err)
	}
	bundle.CACert, err = ioutil.TempFile(dirName, "ca-cert-")
	if err != nil {
		panic(err)
	}
	bundle.CAKey, err = ioutil.TempFile(dirName, "ca-key-")
	if err != nil {
		panic(err)
	}
	bundle.ServerCert, err = ioutil.TempFile(dirName, "server-cert-")
	if err != nil {
		panic(err)
	}
	bundle.ServerKey, err = ioutil.TempFile(dirName, "server-key-")
	if err != nil {
		panic(err)
	}
	bundle.ClientCert, err = ioutil.TempFile(dirName, "client-cert-")
	if err != nil {
		panic(err)
	}
	bundle.ClientKey, err = ioutil.TempFile("", "client-key-")
	if err != nil {
		panic(err)
	}
	// generate certs
	catls, err := generateCA(bundle.CACert, bundle.CAKey)
	if err != nil {
		panic(err)
	}
	err = generateCert(catls, bundle.ServerCert, bundle.ServerKey)
	if err != nil {
		panic(err)
	}
	err = generateCert(catls, bundle.ClientCert, bundle.ClientKey)
	if err != nil {
		panic(err)
	}
	return bundle
}

func (bundle *CertsBundle) Close() {
	_ = os.Remove(bundle.CACert.Name())
	_ = os.Remove(bundle.CAKey.Name())
	_ = os.Remove(bundle.ServerCert.Name())
	_ = os.Remove(bundle.ServerKey.Name())
	_ = os.Remove(bundle.ClientCert.Name())
	_ = os.Remove(bundle.ClientKey.Name())
	_ = os.Remove(bundle.dirName)
}
