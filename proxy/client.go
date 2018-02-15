package proxy

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"github.com/grepplabs/kafka-proxy/config"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"net"
	"time"
)

const (
	keepAlivePeriod = time.Minute
)

// Conn represents a connection from a client to a specific instance.
type Conn struct {
	BrokerAddress   string
	LocalConnection net.Conn
}

// Client is a type to handle connecting to a Server. All fields are required
// unless otherwise specified.
type Client struct {
	Conns *ConnSet
	// Dialer should return a new connection to the provided address. It is
	// called on each new connection to an instance. net.Dial will be used if
	// left nil.
	Dialer func(net, addr string) (net.Conn, error)
	// Config of Proxy request-response processor (instance p)
	ProcessorConfig ProcessorConfig
}

// Run causes the client to start waiting for new connections to connSrc and
// proxy them to the destination instance. It blocks until connSrc is closed.
func (c *Client) Run(stopChan <-chan struct{}, connSrc <-chan Conn) {
STOP:
	for {
		select {
		case conn := <-connSrc:
			go c.handleConn(conn)
		case <-stopChan:
			break STOP
		}
	}

	log.Print("Closing connections")

	if err := c.Conns.Close(); err != nil {
		log.Printf("closing client had error: %v", err)
	}

	log.Print("Proxy is stopped")
}

func (c *Client) handleConn(conn Conn) {
	//TODO: add NaxConnections ?
	server, err := c.DialNoTLS(conn.BrokerAddress)
	if err != nil {
		log.Printf("couldn't connect to %q: %v", conn.BrokerAddress, err)
		conn.LocalConnection.Close()
		return
	}

	c.Conns.Add(conn.BrokerAddress, conn.LocalConnection)
	copyThenClose(c.ProcessorConfig, server, conn.LocalConnection, conn.BrokerAddress, "local connection on "+conn.LocalConnection.LocalAddr().String())
	if err := c.Conns.Remove(conn.BrokerAddress, conn.LocalConnection); err != nil {
		log.Print(err)
	}
}

// tryConnect
func (c *Client) DialNoTLS(brokerAddress string) (net.Conn, error) {
	//TODO: if TLS provided configure keepAlive and timeouts

	d := c.Dialer
	if d == nil {
		d = net.Dial
	}
	conn, err := d("tcp", brokerAddress)
	if err != nil {
		return nil, err
	}
	type setKeepAliver interface {
		SetKeepAlive(keepalive bool) error
		SetKeepAlivePeriod(d time.Duration) error
	}

	if s, ok := conn.(setKeepAliver); ok {
		if err := s.SetKeepAlive(true); err != nil {
			log.Printf("Couldn't set KeepAlive to true: %v", err)
		} else if err := s.SetKeepAlivePeriod(keepAlivePeriod); err != nil {
			log.Printf("Couldn't set KeepAlivePeriod to %v", keepAlivePeriod)
		}
	} else {
		log.Printf("KeepAlive not supported: long-running tcp connections may be killed by the OS.")
	}

	return conn, nil
}

func (c *Client) DialTLS(brokerAddress string) (net.Conn, error) {
	//TODO:
	conf := config.NewConfig()
	conf.Kafka.TLS.Enable = true
	conf.Kafka.SASL.Enable = true
	conf.Kafka.SASL.Username = ""
	conf.Kafka.SASL.Password = ""
	conf.Kafka.TLS.InsecureSkipVerify = true

	dialer := net.Dialer{
		Timeout:   conf.Kafka.DialTimeout,
		KeepAlive: conf.Kafka.KeepAlive,
	}
	if conf.Kafka.TLS.Enable {
		tlsConfig, err := newTLSConfig(conf)
		if err != nil {
			panic(err)
		}
		conn, connErr := tls.DialWithDialer(&dialer, "tcp", brokerAddress, tlsConfig)
		if connErr != nil {
			return nil, connErr
		}
		if conf.Kafka.SASL.Enable {
			// http://kafka.apache.org/protocol.html#sasl_handshake
			saslPlainAuth := SASLPlainAuth{
				conn:         conn,
				writeTimeout: conf.Kafka.WriteTimeout,
				readTimeout:  conf.Kafka.ReadTimeout,
				username:     conf.Kafka.SASL.Username,
				password:     conf.Kafka.SASL.Password,
			}
			err = saslPlainAuth.sendAndReceiveSASLPlainAuth()
			if err != nil {
				conn.Close()
				return nil, err
			}
			// reset deadlines
			conn.SetWriteDeadline(time.Time{})
			conn.SetReadDeadline(time.Time{})
		}
		return conn, connErr
	} else {
		return dialer.Dial("tcp", brokerAddress)
	}
	//
	return nil, nil
}

func newTLSConfig(conf *config.Config) (*tls.Config, error) {
	opts := conf.Kafka.TLS

	cfg := &tls.Config{InsecureSkipVerify: opts.InsecureSkipVerify}

	if opts.ClientCertFile != "" && opts.ClientKeyFile != "" {
		certPEMBlock, err := ioutil.ReadFile(opts.ClientCertFile)
		if err != nil {
			return nil, err
		}
		keyPEMBlock, err := ioutil.ReadFile(opts.ClientKeyFile)
		if err != nil {
			return nil, err
		}
		keyPEMBlock, err = decryptPEM(keyPEMBlock, opts.ClientKeyPassword)
		if err != nil {
			return nil, err
		}
		cert, err := tls.X509KeyPair(certPEMBlock, keyPEMBlock)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
		cfg.BuildNameToCertificate()
	}

	if opts.CAChainCertFile != "" {
		caCert, err := ioutil.ReadFile(opts.CAChainCertFile)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		cfg.RootCAs = caCertPool
	}
	return cfg, nil
}

func decryptPEM(pemData []byte, password string) ([]byte, error) {

	keyBlock, _ := pem.Decode(pemData)
	if keyBlock == nil {
		return nil, errors.New("Failed to parse PEM")
	}
	if x509.IsEncryptedPEMBlock(keyBlock) {
		if password == "" {
			return nil, errors.New("PEM is encrypted, but password is empty")
		}
		key, err := x509.DecryptPEMBlock(keyBlock, []byte(password))
		if err != nil {
			return nil, err
		}
		block := &pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: key,
		}
		return pem.EncodeToMemory(block), nil
	}
	return pemData, nil
}
