package proxy

import (
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
	server, err := c.DialNoSSL(conn.BrokerAddress)
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
func (c *Client) DialNoSSL(instance string) (net.Conn, error) {
	//TODO: if TLS provided configure keepAlive and timeouts

	d := c.Dialer
	if d == nil {
		d = net.Dial
	}
	conn, err := d("tcp", instance)
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
