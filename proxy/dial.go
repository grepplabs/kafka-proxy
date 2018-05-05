package proxy

import (
	"bufio"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"github.com/pkg/errors"
	"golang.org/x/net/proxy"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Dialer interface {
	Dial(network, addr string) (c net.Conn, err error)
}

type directDialer struct {
	dialTimeout time.Duration
	keepAlive   time.Duration
}

func (d directDialer) Dial(network, addr string) (net.Conn, error) {
	dialer := net.Dialer{
		Timeout:   d.dialTimeout,
		KeepAlive: d.keepAlive,
	}
	conn, err := dialer.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	err = conn.SetDeadline(time.Now().Add(d.dialTimeout))
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, err
}

type socks5Dialer struct {
	directDialer            directDialer
	proxyNetwork, proxyAddr string
	username, password      string
}

func (d socks5Dialer) Dial(network, addr string) (net.Conn, error) {
	if d.proxyNetwork == "" || d.proxyAddr == "" {
		return nil, errors.New("socks5 proxy network and addr must be not empty")
	}
	var auth *proxy.Auth
	if d.username != "" && d.password != "" {
		auth = &proxy.Auth{
			User:     d.username,
			Password: d.password,
		}
	}
	socks5Dialer, err := proxy.SOCKS5(d.proxyNetwork, d.proxyAddr, auth, d.directDialer)
	if err != nil {
		return nil, err
	}
	conn, err := socks5Dialer.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

type tlsDialer struct {
	timeout   time.Duration
	rawDialer Dialer
	config    *tls.Config
}

// see tls.DialWithDialer
func (d tlsDialer) Dial(network, addr string) (net.Conn, error) {
	if d.config == nil {
		return nil, errors.New("tlsConfig must not be nil")
	}
	if d.rawDialer == nil {
		return nil, errors.New("rawDialer must not be nil")
	}

	timeout := d.timeout

	var errChannel chan error

	if timeout != 0 {
		errChannel = make(chan error, 2)
		timer := time.AfterFunc(timeout, func() {
			errChannel <- errors.Errorf("Handshake timeout to %s after %v", addr, timeout)
		})
		defer timer.Stop()
	}

	rawConn, err := d.rawDialer.Dial(network, addr)
	if err != nil {
		return nil, err
	}

	colonPos := strings.LastIndex(addr, ":")
	if colonPos == -1 {
		colonPos = len(addr)
	}
	hostname := addr[:colonPos]

	config := d.config

	// If no ServerName is set, infer the ServerName
	// from the hostname we're connecting to.
	if config.ServerName == "" {
		// Make a copy to avoid polluting argument or default.
		c := config.Clone()
		c.ServerName = hostname
		config = c
	}

	conn := tls.Client(rawConn, config)

	if timeout == 0 {
		err = conn.Handshake()
	} else {
		go func() {
			errChannel <- conn.Handshake()
		}()

		err = <-errChannel
	}

	if err != nil {
		rawConn.Close()
		return nil, err
	}

	return conn, nil
}

type httpProxy struct {
	forwardDialer      Dialer
	network            string
	hostPort           string
	username, password string
}

func (s *httpProxy) Dial(network, addr string) (net.Conn, error) {
	reqURL, err := url.Parse("http://" + addr)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("CONNECT", reqURL.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Close = false
	if s.username != "" && s.password != "" {
		basic := "Basic " + base64.StdEncoding.EncodeToString([]byte(s.username+":"+s.password))
		req.Header.Set("Proxy-Authorization", basic)
	}

	c, err := s.forwardDialer.Dial(s.network, s.hostPort)
	if err != nil {
		return nil, err
	}
	err = req.Write(c)
	if err != nil {
		c.Close()
		return nil, err
	}

	resp, err := http.ReadResponse(bufio.NewReader(c), req)
	if err != nil {
		c.Close()
		return nil, err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		c.Close()
		return nil, fmt.Errorf("connect server using proxy error, statuscode [%d]", resp.StatusCode)
	}

	return c, nil
}
