package proxy

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
	"time"
)

func TestAuthHandshake(t *testing.T) {
	a := assert.New(t)

	magic, err := Uint64()
	a.Nil(err)

	fmt.Println(magic)

	client := &AuthClient{
		enabled: true,
		magic:   magic,
		method:  "google-id",
		timeout: 10 * time.Second,
	}

	server := &AuthServer{
		enabled: true,
		magic:   magic,
		method:  "google-id",
		timeout: 10 * time.Second,
	}
	c1, c2, _, err := makePipe()
	a.Nil(err)

	//TODO: check recieved toke and bytes dataCh <- wr.Bytes()
	//     sendAndReceiveGatewayAuth <=> can return 4 bytes to check
	//TODO: pipe writes to c1, c2
	go func() {
		cerr := client.sendAndReceiveGatewayAuth(c1)
		a.Nil(cerr)
	}()
	serr := server.receiveAndSendGatewayAuth(c2)
	a.Nil(serr)
}

func makePipe() (c1, c2 net.Conn, stop func(), err error) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, nil, nil, err
	}

	// Start a connection between two endpoints.
	var err1, err2 error
	done := make(chan bool)
	go func() {
		c2, err2 = ln.Accept()
		close(done)
	}()
	c1, err1 = net.Dial(ln.Addr().Network(), ln.Addr().String())
	<-done

	stop = func() {
		if err1 == nil {
			c1.Close()
		}
		if err2 == nil {
			c2.Close()
		}
		ln.Close()
	}

	switch {
	case err1 != nil:
		stop()
		return nil, nil, nil, err1
	case err2 != nil:
		stop()
		return nil, nil, nil, err2
	default:
		return c1, c2, stop, nil
	}
}

func Uint64() (uint64, error) {
	var b [8]byte

	_, err := rand.Read(b[:])
	if err != nil {
		return uint64(0), err
	}

	return binary.LittleEndian.Uint64(b[:]), nil
}
