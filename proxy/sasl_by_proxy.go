package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"github.com/sirupsen/logrus"
)

const (
	SASLPlain       = "PLAIN"
	SASLOAuthBearer = "OAUTHBEARER"
	SASLSCRAM256    = "SCRAM-SHA-256"
	SASLSCRAM512    = "SCRAM-SHA-512"
	SASLSGSSAPI     = "GSSAPI"
	SASLIAMAAUTH    = "AWS_MSK_IAM"
)

type SASLHandshake struct {
	clientID  string
	version   int16
	mechanism string

	writeTimeout time.Duration
	readTimeout  time.Duration
}

type SASLOAuthBearerAuth struct {
	clientID string

	writeTimeout time.Duration
	readTimeout  time.Duration

	tokenProvider apis.TokenProvider
}

type SASLPlainAuth struct {
	clientID string

	writeTimeout time.Duration
	readTimeout  time.Duration

	username string
	password string
}

type SASLAuthByProxy interface {
	sendAndReceiveSASLAuth(conn DeadlineReaderWriter, brokerAddress string) error
}

// In SASL Plain, Kafka expects the auth header to be in the following format
// Message format (from https://tools.ietf.org/html/rfc4616):
//
//	message   = [authzid] UTF8NUL authcid UTF8NUL passwd
//	authcid   = 1*SAFE ; MUST accept up to 255 octets
//	authzid   = 1*SAFE ; MUST accept up to 255 octets
//	passwd    = 1*SAFE ; MUST accept up to 255 octets
//	UTF8NUL   = %x00 ; UTF-8 encoded NUL character
//
//	SAFE      = UTF1 / UTF2 / UTF3 / UTF4
//	               ;; any UTF-8 encoded Unicode character except NUL
//
// When credentials are valid, Kafka returns a 4 byte array of null characters.
// When credentials are invalid, Kafka closes the connection. This does not seem to be the ideal way
// of responding to bad credentials but thats how its being done today.
func (b *SASLPlainAuth) sendAndReceiveSASLAuth(conn DeadlineReaderWriter, _ string) error {

	saslHandshake := &SASLHandshake{
		clientID:     b.clientID,
		version:      0,
		mechanism:    SASLPlain,
		writeTimeout: b.writeTimeout,
		readTimeout:  b.readTimeout,
	}
	handshakeErr := saslHandshake.sendAndReceiveHandshake(conn)
	if handshakeErr != nil {
		return handshakeErr
	}
	return b.sendSaslAuthenticateRequest(conn)
}

func (b *SASLPlainAuth) sendSaslAuthenticateRequest(conn DeadlineReaderWriter) error {
	logrus.Debugf("Sending authentication opaque packets, mechanism PLAIN")

	length := 1 + len(b.username) + 1 + len(b.password)
	authBytes := make([]byte, length+4) //4 byte length header + auth data
	binary.BigEndian.PutUint32(authBytes, uint32(length))
	copy(authBytes[4:], []byte("\x00"+b.username+"\x00"+b.password))

	err := conn.SetWriteDeadline(time.Now().Add(b.writeTimeout))
	if err != nil {
		return err
	}
	_, err = conn.Write(authBytes)
	if err != nil {
		return fmt.Errorf("failed to write SASL auth header: %w", err)
	}

	err = conn.SetReadDeadline(time.Now().Add(b.readTimeout))
	if err != nil {
		return err
	}

	header := make([]byte, 4)
	_, err = io.ReadFull(conn, header)
	// If the credentials are valid, we would get a 4 byte response filled with null characters.
	// Otherwise, the broker closes the connection and we get an EOF
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("SASL/PLAIN auth for user %s failed", b.username)
		}
		return fmt.Errorf("failed to read response while authenticating with SASL: %w", err)
	}
	return nil
}

func (b *SASLHandshake) sendAndReceiveHandshake(conn DeadlineReaderWriter) error {
	logrus.Debugf("Sending SaslHandshakeRequest mechanism: %v  version: %v", b.mechanism, b.version)
	req := &protocol.Request{
		ClientID: b.clientID,
		Body:     &protocol.SaslHandshakeRequestV0orV1{Version: b.version, Mechanism: b.mechanism},
	}
	reqBuf, err := protocol.Encode(req)
	if err != nil {
		return err
	}
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(reqBuf)))

	err = conn.SetWriteDeadline(time.Now().Add(b.writeTimeout))
	if err != nil {
		return err
	}

	_, err = conn.Write(bytes.Join([][]byte{sizeBuf, reqBuf}, nil))
	if err != nil {
		return fmt.Errorf("failed to send SASL handshake: %w", err)
	}

	err = conn.SetReadDeadline(time.Now().Add(b.readTimeout))
	if err != nil {
		return err
	}

	//wait for the response
	header := make([]byte, 8) // response header
	_, err = io.ReadFull(conn, header)
	if err != nil {
		return fmt.Errorf("failed to read SASL handshake header: %w", err)
	}
	length := binary.BigEndian.Uint32(header[:4])
	payload := make([]byte, length-4)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		return fmt.Errorf("failed to read SASL handshake payload: %w", err)
	}
	res := &protocol.SaslHandshakeResponseV0orV1{}
	err = protocol.Decode(payload, res)
	if err != nil {
		return fmt.Errorf("failed to parse SASL handshake: %w", err)
	}
	if !errors.Is(res.Err, protocol.ErrNoError) {
		return fmt.Errorf("invalid SASL Mechanism: %w", res.Err)
	}
	logrus.Debugf("Successful SASL handshake. Available mechanisms: %v", res.EnabledMechanisms)
	return nil
}

func (b *SASLOAuthBearerAuth) getOAuthBearerToken() (string, error) {
	resp, err := b.tokenProvider.GetToken(context.Background(), apis.TokenRequest{})
	if err != nil {
		return "", err
	}
	if !resp.Success {
		return "", fmt.Errorf("get sasl token failed with status: %d", resp.Status)
	}
	if resp.Token == "" {
		return "", errors.New("get sasl token returned empty token")
	}
	return resp.Token, nil
}

func (b *SASLOAuthBearerAuth) sendAndReceiveSASLAuth(conn DeadlineReaderWriter, _ string) error {

	token, err := b.getOAuthBearerToken()
	if err != nil {
		return err
	}
	saslHandshake := &SASLHandshake{
		clientID:     b.clientID,
		version:      1,
		mechanism:    SASLOAuthBearer,
		writeTimeout: b.writeTimeout,
		readTimeout:  b.readTimeout,
	}
	handshakeErr := saslHandshake.sendAndReceiveHandshake(conn)
	if handshakeErr != nil {
		return handshakeErr
	}
	return b.sendSaslAuthenticateRequest(token, conn)
}

func (b *SASLOAuthBearerAuth) sendSaslAuthenticateRequest(token string, conn DeadlineReaderWriter) error {
	logrus.Debugf("Sending SaslAuthenticateRequest, mechanism OAUTHBEARER")

	saslAuthReqV0 := protocol.SaslAuthenticateRequestV0{SaslAuthBytes: SaslOAuthBearer{}.ToBytes(token, "", make(map[string]string, 0))}

	req := &protocol.Request{
		ClientID: b.clientID,
		Body:     &saslAuthReqV0,
	}
	reqBuf, err := protocol.Encode(req)
	if err != nil {
		return err
	}
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, uint32(len(reqBuf)))

	err = conn.SetWriteDeadline(time.Now().Add(b.writeTimeout))
	if err != nil {
		return err
	}

	_, err = conn.Write(bytes.Join([][]byte{sizeBuf, reqBuf}, nil))
	if err != nil {
		return fmt.Errorf("failed to send SASL auth request: %w", err)
	}

	err = conn.SetReadDeadline(time.Now().Add(b.readTimeout))
	if err != nil {
		return err
	}

	//wait for the response
	header := make([]byte, 8) // response header
	_, err = io.ReadFull(conn, header)
	if err != nil {
		return fmt.Errorf("failed to read SASL auth header: %w", err)
	}
	length := binary.BigEndian.Uint32(header[:4])
	payload := make([]byte, length-4)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		return fmt.Errorf("failed to read SASL auth payload: %w", err)
	}

	res := &protocol.SaslAuthenticateResponseV0{}
	err = protocol.Decode(payload, res)
	if err != nil {
		return fmt.Errorf("failed to parse SASL auth response: %w", err)
	}
	if !errors.Is(res.Err, protocol.ErrNoError) {
		return fmt.Errorf("SASL authentication failed, error message is '%v'", res.ErrMsg)
	}
	return nil
}
