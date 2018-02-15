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

// Conn represents a connection from a client to a specific instance.
type Conn struct {
	BrokerAddress   string
	LocalConnection net.Conn
}

// Client is a type to handle connecting to a Server. All fields are required
// unless otherwise specified.
type Client struct {
	conns *ConnSet

	// Kafka Net configuration
	config *config.Config

	// Config of Proxy request-response processor (instance p)
	processorConfig ProcessorConfig

	tlsConfig *tls.Config
}

func NewClient(conns *ConnSet, c *config.Config, netAddressMappingFunc config.NetAddressMappingFunc) (*Client, error) {
	tlsConfig, err := newTLSConfig(c)
	if err != nil {
		return nil, err
	}
	return &Client{conns: conns, config: c, tlsConfig: tlsConfig, processorConfig: ProcessorConfig{MaxOpenRequests: c.Kafka.MaxOpenRequests, NetAddressMappingFunc: netAddressMappingFunc}}, nil
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

	if err := c.conns.Close(); err != nil {
		log.Printf("closing client had error: %v", err)
	}

	log.Print("Proxy is stopped")
}

func (c *Client) handleConn(conn Conn) {
	//TODO: add NaxConnections ?
	server, err := c.Dial(conn.BrokerAddress, c.tlsConfig)
	if err != nil {
		log.Printf("couldn't connect to %q: %v", conn.BrokerAddress, err)
		conn.LocalConnection.Close()
		return
	}

	c.conns.Add(conn.BrokerAddress, conn.LocalConnection)
	copyThenClose(c.processorConfig, server, conn.LocalConnection, conn.BrokerAddress, "local connection on "+conn.LocalConnection.LocalAddr().String())
	if err := c.conns.Remove(conn.BrokerAddress, conn.LocalConnection); err != nil {
		log.Print(err)
	}
}

func (c *Client) Dial(brokerAddress string, tlsConfig *tls.Config) (net.Conn, error) {
	dialer := net.Dialer{
		Timeout:   c.config.Kafka.DialTimeout,
		KeepAlive: c.config.Kafka.KeepAlive,
	}
	if c.config.Kafka.TLS.Enable {
		if tlsConfig == nil {
			return nil, errors.New("tlsConfig must not be nil")
		}
		conn, connErr := tls.DialWithDialer(&dialer, "tcp", brokerAddress, tlsConfig)
		if connErr != nil {
			return nil, connErr
		}
		if c.config.Kafka.SASL.Enable {
			// http://kafka.apache.org/protocol.html#sasl_handshake
			saslPlainAuth := SASLPlainAuth{
				conn:         conn,
				clientID:     c.config.Kafka.ClientID,
				writeTimeout: c.config.Kafka.WriteTimeout,
				readTimeout:  c.config.Kafka.ReadTimeout,
				username:     c.config.Kafka.SASL.Username,
				password:     c.config.Kafka.SASL.Password,
			}
			err := saslPlainAuth.sendAndReceiveSASLPlainAuth()
			if err != nil {
				conn.Close()
				return nil, err
			}
			// reset deadlines
			conn.SetDeadline(time.Time{})
		}
		return conn, connErr
	}
	return dialer.Dial("tcp", brokerAddress)
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
