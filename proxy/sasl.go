package proxy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"github.com/pkg/errors"
	"io"
	"time"
)

const (
	SASLPlain       = "PLAIN"
	SASLOAuthBearer = "OAUTHBEARER"
)

type SASLPlainAuth struct {
	clientID string

	writeTimeout time.Duration
	readTimeout  time.Duration

	username string
	password string
}

// In SASL Plain, Kafka expects the auth header to be in the following format
// Message format (from https://tools.ietf.org/html/rfc4616):
//
//   message   = [authzid] UTF8NUL authcid UTF8NUL passwd
//   authcid   = 1*SAFE ; MUST accept up to 255 octets
//   authzid   = 1*SAFE ; MUST accept up to 255 octets
//   passwd    = 1*SAFE ; MUST accept up to 255 octets
//   UTF8NUL   = %x00 ; UTF-8 encoded NUL character
//
//   SAFE      = UTF1 / UTF2 / UTF3 / UTF4
//                  ;; any UTF-8 encoded Unicode character except NUL
//
// When credentials are valid, Kafka returns a 4 byte array of null characters.
// When credentials are invalid, Kafka closes the connection. This does not seem to be the ideal way
// of responding to bad credentials but thats how its being done today.
func (b *SASLPlainAuth) sendAndReceiveSASLPlainAuth(conn DeadlineReaderWriter) error {

	handshakeErr := b.sendAndReceiveSASLPlainHandshake(conn)
	if handshakeErr != nil {
		return handshakeErr
	}
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
		return errors.Wrap(err, "Failed to write SASL auth header")
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
		return errors.Wrap(err, "Failed to read response while authenticating with SASL")
	}
	return nil
}

func (b *SASLPlainAuth) sendAndReceiveSASLPlainHandshake(conn DeadlineReaderWriter) error {

	req := &protocol.Request{
		ClientID: b.clientID,
		Body:     &protocol.SaslHandshakeRequestV0orV1{Version: 0, Mechanism: SASLPlain},
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
		return errors.Wrap(err, "Failed to send SASL handshake")
	}

	err = conn.SetReadDeadline(time.Now().Add(b.readTimeout))
	if err != nil {
		return err
	}

	//wait for the response
	header := make([]byte, 8) // response header
	_, err = io.ReadFull(conn, header)
	if err != nil {
		return errors.Wrap(err, "Failed to read SASL handshake header")
	}
	length := binary.BigEndian.Uint32(header[:4])
	payload := make([]byte, length-4)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		return errors.Wrap(err, "Failed to read SASL handshake payload")
	}
	res := &protocol.SaslHandshakeResponseV0orV1{}
	err = protocol.Decode(payload, res)
	if err != nil {
		return errors.Wrap(err, "Failed to parse SASL handshake")
	}
	if res.Err != protocol.ErrNoError {
		return errors.Wrap(res.Err, "Invalid SASL Mechanism")
	}
	return nil
}
