package proxy

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"strings"
	"time"
)

type AuthClient struct {
	enabled bool
	magic   uint64
	method  string
	timeout time.Duration
}

//TODO: reset deadlines after method - ok
func (b *AuthClient) sendAndReceiveGatewayAuth(conn DeadlineReaderWriter) error {
	//TODO: retrieve from plugin (with timeout)
	data := "my-test-jwt-token"

	length := len(b.method) + 1 + len(data)
	// 8 - bytes magic, 4 bytes length
	buf := make([]byte, 12+length)
	binary.BigEndian.PutUint64(buf[:8], b.magic)
	binary.BigEndian.PutUint32(buf[8:], uint32(length))
	copy(buf[12:], []byte(b.method+"\x00"+data))

	err := conn.SetDeadline(time.Now().Add(b.timeout))
	if err != nil {
		return err
	}
	_, err = conn.Write(buf)
	if err != nil {
		return errors.Wrap(err, "Failed to write gateway handshake")
	}

	header := make([]byte, 4)
	_, err = io.ReadFull(conn, header)
	// If the credentials are valid, we would get a 4 byte response filled with null characters.
	// Otherwise, the broker closes the connection and we get an EOF
	if err != nil {
		if err == io.EOF {
			return errors.New("Gateway auth failed")
		}
		return errors.Wrap(err, "Failed to read response while gateway authenticating")
	}
	return nil
}

type AuthServer struct {
	enabled bool
	magic   uint64
	method  string
	timeout time.Duration
}

//TODO: reset deadlines after method - ok
func (b *AuthServer) receiveAndSendGatewayAuth(conn DeadlineReaderWriter) error {
	err := conn.SetDeadline(time.Now().Add(b.timeout))
	if err != nil {
		return err
	}
	headerBuf := make([]byte, 12) // magic 8 + length 4
	_, err = io.ReadFull(conn, headerBuf)
	if err != nil {
		return errors.Wrap(err, "Failed to read gateway bytes magic")
	}

	magic := binary.BigEndian.Uint64(headerBuf[:8])
	if magic != b.magic {
		return errors.Wrap(err, "gateway handshake magic bytes mismatch")
	}

	length := binary.BigEndian.Uint32(headerBuf[8:])
	payload := make([]byte, length-4)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		return errors.Wrap(err, "Failed to read gateway handshake payload")
	}
	tokens := strings.Split(string(payload), "\x00")
	if len(tokens) != 2 {
		return fmt.Errorf("invalid gateway handshake: expected 2 tokens, got %d", len(tokens))
	}
	if tokens[0] != b.method {
		return errors.Wrap(err, fmt.Sprintf("gateway handshake method mismatch: expected %s , got %s", b.method, tokens[0]))
	}
	data := tokens[1]
	// TODO: use data for authentication
	_ = data

	logrus.Infof("gateway handshake payload: %s", data)

	header := make([]byte, 4)
	if _, err := conn.Write(header); err != nil {
		return err
	}
	return nil
}
