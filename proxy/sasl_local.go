package proxy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/grepplabs/kafka-proxy/plugin/local-auth/shared"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"io"
	"strconv"
	"strings"
	"time"
)

type LocalSasl struct {
	enabled            bool
	timeout            time.Duration
	localAuthenticator shared.PasswordAuthenticator
}

func (p *LocalSasl) receiveAndSendSASLPlainAuth(conn DeadlineReaderWriter) (err error) {
	if err = p.receiveAndSendSasl(conn); err != nil {
		return err
	}
	if err = p.receiveAndSendAuth(conn); err != nil {
		return err
	}
	return nil
}

func (p *LocalSasl) receiveAndSendSasl(conn DeadlineReaderWriter) (err error) {
	requestDeadline := time.Now().Add(p.timeout)
	err = conn.SetDeadline(requestDeadline)
	if err != nil {
		return err
	}

	keyVersionBuf := make([]byte, 8) // Size => int32 + ApiKey => int16 + ApiVersion => int16
	if _, err = io.ReadFull(conn, keyVersionBuf); err != nil {
		return err
	}
	requestKeyVersion := &protocol.RequestKeyVersion{}
	if err = protocol.Decode(keyVersionBuf, requestKeyVersion); err != nil {
		return err
	}
	if !(requestKeyVersion.ApiKey == 17 && requestKeyVersion.ApiVersion == 0) {
		return errors.New("SaslHandshake version 0 is expected")
	}

	if int32(requestKeyVersion.Length) > protocol.MaxRequestSize {
		return protocol.PacketDecodingError{Info: fmt.Sprintf("sasl handshake message of length %d too large", requestKeyVersion.Length)}
	}

	resp := make([]byte, int(requestKeyVersion.Length-4))
	if _, err = io.ReadFull(conn, resp); err != nil {
		return err
	}
	payload := bytes.Join([][]byte{keyVersionBuf[4:], resp}, nil)

	saslReqV0 := &protocol.SaslHandshakeRequestV0{}
	req := &protocol.Request{Body: saslReqV0}
	if err = protocol.Decode(payload, req); err != nil {
		return err
	}

	var saslResult error
	saslErr := protocol.ErrNoError
	if saslReqV0.Mechanism != SASLPlain {
		saslResult = fmt.Errorf("PLAIN mechanism expected, but got %s", saslReqV0.Mechanism)
		saslErr = protocol.ErrUnsupportedSASLMechanism
	}

	saslResV0 := &protocol.SaslHandshakeResponseV0{Err: saslErr, EnabledMechanisms: []string{SASLPlain}}
	newResponseBuf, err := protocol.Encode(saslResV0)
	if err != nil {
		return err
	}
	newHeaderBuf, err := protocol.Encode(&protocol.ResponseHeader{Length: int32(len(newResponseBuf) + 4), CorrelationID: req.CorrelationID})
	if err != nil {
		return err
	}
	if _, err := conn.Write(newHeaderBuf); err != nil {
		return err
	}
	if _, err := conn.Write(newResponseBuf); err != nil {
		return err
	}
	return saslResult
}

func (p *LocalSasl) receiveAndSendAuth(conn DeadlineReaderWriter) (err error) {
	requestDeadline := time.Now().Add(p.timeout)
	err = conn.SetDeadline(requestDeadline)
	if err != nil {
		return err
	}

	sizeBuf := make([]byte, 4) // Size => int32
	if _, err = io.ReadFull(conn, sizeBuf); err != nil {
		return err
	}

	length := binary.BigEndian.Uint32(sizeBuf)
	if int32(length) > protocol.MaxRequestSize {
		return protocol.PacketDecodingError{Info: fmt.Sprintf("auth message of length %d too large", length)}
	}

	payload := make([]byte, length)
	_, err = io.ReadFull(conn, payload)
	if err != nil {
		return err
	}
	tokens := strings.Split(string(payload), "\x00")
	if len(tokens) != 3 {
		return fmt.Errorf("invalid SASL/PLAIN request: expected 3 tokens, got %d", len(tokens))
	}
	if p.localAuthenticator == nil {
		return protocol.PacketDecodingError{Info: "Listener authenticator is not set"}
	}

	// logrus.Infof("user: %s , password: %s", tokens[1], tokens[2])
	ok, status, err := p.localAuthenticator.Authenticate(tokens[1], tokens[2])
	if err != nil {
		proxyLocalAuthTotal.WithLabelValues("error", "1").Inc()
		return err
	}
	proxyLocalAuthTotal.WithLabelValues(strconv.FormatBool(ok), strconv.Itoa(int(status))).Inc()

	if !ok {
		return fmt.Errorf("user %s authentication failed", tokens[1])
	}
	// If the credentials are valid, we would write a 4 byte response filled with null characters.
	// Otherwise, the closes the connection i.e. return error
	header := make([]byte, 4)
	if _, err := conn.Write(header); err != nil {
		return err
	}
	return nil
}
