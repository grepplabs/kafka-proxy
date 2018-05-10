package proxy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"io"
	"strconv"
	"strings"
	"time"
)

type LocalSasl struct {
	enabled            bool
	timeout            time.Duration
	localAuthenticator apis.PasswordAuthenticator
}

func (p *LocalSasl) receiveAndSendSASLPlainAuthV1(conn DeadlineReaderWriter, readKeyVersionBuf []byte) (err error) {
	if err = p.receiveAndSendSaslV0orV1(conn, readKeyVersionBuf, 1); err != nil {
		return err
	}
	if err = p.receiveAndSendAuthV1(conn); err != nil {
		return err
	}
	return nil
}

func (p *LocalSasl) receiveAndSendSASLPlainAuthV0(conn DeadlineReaderWriter, readKeyVersionBuf []byte) (err error) {
	if err = p.receiveAndSendSaslV0orV1(conn, readKeyVersionBuf, 0); err != nil {
		return err
	}
	if err = p.receiveAndSendAuthV0(conn); err != nil {
		return err
	}
	return nil
}

func (p *LocalSasl) receiveAndSendSaslV0orV1(conn DeadlineReaderWriter, keyVersionBuf []byte, version int16) (err error) {
	requestDeadline := time.Now().Add(p.timeout)
	err = conn.SetDeadline(requestDeadline)
	if err != nil {
		return err
	}

	if len(keyVersionBuf) != 8 {
		return errors.New("length of keyVersionBuf should be 8")
	}
	// keyVersionBuf has already been read from connection
	requestKeyVersion := &protocol.RequestKeyVersion{}
	if err = protocol.Decode(keyVersionBuf, requestKeyVersion); err != nil {
		return err
	}
	if !(requestKeyVersion.ApiKey == 17 && requestKeyVersion.ApiVersion == version) {
		return fmt.Errorf("SaslHandshake version %d is expected, but got %d", version, requestKeyVersion.ApiVersion)
	}

	if int32(requestKeyVersion.Length) > protocol.MaxRequestSize {
		return protocol.PacketDecodingError{Info: fmt.Sprintf("sasl handshake message of length %d too large", requestKeyVersion.Length)}
	}

	resp := make([]byte, int(requestKeyVersion.Length-4))
	if _, err = io.ReadFull(conn, resp); err != nil {
		return err
	}
	payload := bytes.Join([][]byte{keyVersionBuf[4:], resp}, nil)

	saslReqV0orV1 := &protocol.SaslHandshakeRequestV0orV1{Version: version}
	req := &protocol.Request{Body: saslReqV0orV1}
	if err = protocol.Decode(payload, req); err != nil {
		return err
	}

	var saslResult error
	saslErr := protocol.ErrNoError
	if saslReqV0orV1.Mechanism != SASLPlain {
		saslResult = fmt.Errorf("PLAIN mechanism expected, but got %s", saslReqV0orV1.Mechanism)
		saslErr = protocol.ErrUnsupportedSASLMechanism
	}

	saslResV0 := &protocol.SaslHandshakeResponseV0orV1{Err: saslErr, EnabledMechanisms: []string{SASLPlain}}
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

func (p *LocalSasl) receiveAndSendAuthV1(conn DeadlineReaderWriter) (err error) {
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
	if !(requestKeyVersion.ApiKey == 36 && requestKeyVersion.ApiVersion == 0) {
		return errors.New("SaslAuthenticate version 0 is expected")
	}

	if int32(requestKeyVersion.Length) > protocol.MaxRequestSize {
		return protocol.PacketDecodingError{Info: fmt.Sprintf("sasl authenticate message of length %d too large", requestKeyVersion.Length)}
	}

	resp := make([]byte, int(requestKeyVersion.Length-4))
	if _, err = io.ReadFull(conn, resp); err != nil {
		return err
	}
	payload := bytes.Join([][]byte{keyVersionBuf[4:], resp}, nil)

	saslAuthReqV0 := &protocol.SaslAuthenticateRequestV0{}
	req := &protocol.Request{Body: saslAuthReqV0}
	if err = protocol.Decode(payload, req); err != nil {
		return err
	}

	authErr := p.doLocalAuth(saslAuthReqV0.SaslAuthBytes)

	var saslAuthResV0 *protocol.SaslAuthenticateResponseV0
	if authErr == nil {
		saslAuthResV0 = &protocol.SaslAuthenticateResponseV0{Err: protocol.ErrNoError, SaslAuthBytes: make([]byte, 4)}
	} else {
		errMsg := authErr.Error()
		saslAuthResV0 = &protocol.SaslAuthenticateResponseV0{Err: protocol.ErrSASLAuthenticationFailed, ErrMsg: &errMsg, SaslAuthBytes: make([]byte, 4)}
	}

	newResponseBuf, err := protocol.Encode(saslAuthResV0)
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
	return authErr

}

func (p *LocalSasl) receiveAndSendAuthV0(conn DeadlineReaderWriter) (err error) {
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

	saslAuthBytes := make([]byte, length)
	_, err = io.ReadFull(conn, saslAuthBytes)
	if err != nil {
		return err
	}

	if err = p.doLocalAuth(saslAuthBytes); err != nil {
		return err
	}
	// If the credentials are valid, we would write a 4 byte response filled with null characters.
	// Otherwise, the closes the connection i.e. return error
	header := make([]byte, 4)
	if _, err := conn.Write(header); err != nil {
		return err
	}
	return nil
}

func (p *LocalSasl) doLocalAuth(saslAuthBytes []byte) (err error) {
	tokens := strings.Split(string(saslAuthBytes), "\x00")
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
	return nil
}
