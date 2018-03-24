package proxy

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/grepplabs/kafka-proxy/pkg/apis"
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

	tokenProvider := &testTokenProvider{response: apis.TokenResponse{
		Success: true,
		Token:   "my-test-token",
	}}

	tokenInfo := &testTokenInfo{
		token: "my-test-token",
	}

	client := &AuthClient{
		enabled:       true,
		magic:         magic,
		method:        "google-id",
		timeout:       10 * time.Second,
		tokenProvider: tokenProvider,
	}

	//TODO: implement verify
	server := &AuthServer{
		enabled:   true,
		magic:     magic,
		method:    "google-id",
		timeout:   10 * time.Second,
		tokenInfo: tokenInfo,
	}
	c1, c2, stop, err := makePipe()
	a.Nil(err)
	defer stop()

	clientResult := make(chan error, 1)
	go func() {
		cerr := client.sendAndReceiveGatewayAuth(c1)
		clientResult <- cerr
	}()
	serr := server.receiveAndSendGatewayAuth(c2)
	a.Nil(serr)
	cerr := <-clientResult
	a.Nil(cerr)
}

type testTokenProvider struct {
	response apis.TokenResponse
	err      error
}

// Implements apis.TokenProvider.GetToken
func (p *testTokenProvider) GetToken(ctx context.Context, request apis.TokenRequest) (apis.TokenResponse, error) {
	return p.response, p.err
}

type testTokenInfo struct {
	token string
	err   error
}

// Implements apis.TokenProvider.GetToken
func (p *testTokenInfo) VerifyToken(ctx context.Context, request apis.VerifyRequest) (apis.VerifyResponse, error) {
	if p.token == request.Token {
		return apis.VerifyResponse{Success: true}, p.err
	}
	return apis.VerifyResponse{Success: false}, p.err
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
