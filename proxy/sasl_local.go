package proxy

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"github.com/pkg/errors"

	"io"
	"time"
)

type LocalSasl struct {
	enabled             bool
	timeout             time.Duration
	localAuthenticators map[string]LocalSaslAuth
	UserInfo            string
}

type LocalSaslParams struct {
	enabled               bool
	timeout               time.Duration
	passwordAuthenticator apis.PasswordAuthenticator
	tokenAuthenticator    apis.TokenInfo
}

func NewLocalSasl(params LocalSaslParams) *LocalSasl {
	localAuthenticators := make(map[string]LocalSaslAuth)
	if params.passwordAuthenticator != nil {
		localAuthenticators[SASLPlain] = NewLocalSaslPlain(params.passwordAuthenticator)
	}

	if params.tokenAuthenticator != nil {
		localAuthenticators[SASLOAuthBearer] = NewLocalSaslOauth(params.tokenAuthenticator)
	}
	return &LocalSasl{
		enabled:             params.enabled,
		timeout:             params.timeout,
		localAuthenticators: localAuthenticators,
	}
}

func (p *LocalSasl) receiveAndSendSASLAuthV1(conn DeadlineReaderWriter, readKeyVersionBuf []byte) (err error) {
	var localSaslAuth LocalSaslAuth
	if localSaslAuth, err = p.receiveAndSendSaslV0orV1(conn, readKeyVersionBuf, 1); err != nil {
		return err
	}
	if err = p.receiveAndSendAuthV1(conn, localSaslAuth); err != nil {
		return err
	}
	return nil
}

func (p *LocalSasl) receiveAndSendSASLAuthV0(conn DeadlineReaderWriter, readKeyVersionBuf []byte) (err error) {
	var localSaslAuth LocalSaslAuth
	if localSaslAuth, err = p.receiveAndSendSaslV0orV1(conn, readKeyVersionBuf, 0); err != nil {
		return err
	}
	if err = p.receiveAndSendAuthV0(conn, localSaslAuth); err != nil {
		return err
	}
	return nil
}

func (p *LocalSasl) receiveAndSendSaslV0orV1(conn DeadlineReaderWriter, keyVersionBuf []byte, version int16) (localSaslAuth LocalSaslAuth, err error) {
	requestDeadline := time.Now().Add(p.timeout)
	err = conn.SetDeadline(requestDeadline)
	if err != nil {
		return nil, err
	}

	if len(keyVersionBuf) != 8 {
		return nil, errors.New("length of keyVersionBuf should be 8")
	}
	// keyVersionBuf has already been read from connection
	requestKeyVersion := &protocol.RequestKeyVersion{}
	if err = protocol.Decode(keyVersionBuf, requestKeyVersion); err != nil {
		return nil, err
	}
	if !(requestKeyVersion.ApiKey == 17 && requestKeyVersion.ApiVersion == version) {
		return nil, fmt.Errorf("SaslHandshake version %d is expected, but got %d", version, requestKeyVersion.ApiVersion)
	}

	if int32(requestKeyVersion.Length) > protocol.MaxRequestSize {
		return nil, protocol.PacketDecodingError{Info: fmt.Sprintf("sasl handshake message of length %d too large", requestKeyVersion.Length)}
	}

	resp := make([]byte, int(requestKeyVersion.Length-4))
	if _, err = io.ReadFull(conn, resp); err != nil {
		return nil, err
	}
	payload := bytes.Join([][]byte{keyVersionBuf[4:], resp}, nil)

	saslReqV0orV1 := &protocol.SaslHandshakeRequestV0orV1{Version: version}
	req := &protocol.Request{Body: saslReqV0orV1}
	if err = protocol.Decode(payload, req); err != nil {
		return nil, err
	}

	var saslResult error
	saslErr := protocol.ErrNoError
	localSaslAuth = p.localAuthenticators[saslReqV0orV1.Mechanism]
	if localSaslAuth == nil {
		mechanisms := make([]string, 0)
		for mechanism := range p.localAuthenticators {
			mechanisms = append(mechanisms, mechanism)
		}
		saslResult = fmt.Errorf("PLAIN or OAUTHBEARER mechanism expected, %v are configured, but got %s", mechanisms, saslReqV0orV1.Mechanism)
		saslErr = protocol.ErrUnsupportedSASLMechanism
	}

	saslResV0 := &protocol.SaslHandshakeResponseV0orV1{Err: saslErr, EnabledMechanisms: []string{saslReqV0orV1.Mechanism}}
	newResponseBuf, err := protocol.Encode(saslResV0)
	if err != nil {
		return nil, err
	}
	newHeaderBuf, err := protocol.Encode(&protocol.ResponseHeader{Length: int32(len(newResponseBuf) + 4), CorrelationID: req.CorrelationID})
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(newHeaderBuf); err != nil {
		return nil, err
	}
	if _, err := conn.Write(newResponseBuf); err != nil {
		return nil, err
	}
	return localSaslAuth, saslResult
}

func (p *LocalSasl) receiveAndSendAuthV1(conn DeadlineReaderWriter, localSaslAuth LocalSaslAuth) (err error) {
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
	if requestKeyVersion.ApiKey != 36 {
		return errors.Errorf("SaslAuthenticate is expected, but got apiKey %d", requestKeyVersion.ApiKey)
	}

	if requestKeyVersion.Length > protocol.MaxRequestSize {
		return protocol.PacketDecodingError{Info: fmt.Sprintf("sasl authenticate message of length %d too large", requestKeyVersion.Length)}
	}

	resp := make([]byte, int(requestKeyVersion.Length-4))
	if _, err = io.ReadFull(conn, resp); err != nil {
		return err
	}
	payload := bytes.Join([][]byte{keyVersionBuf[4:], resp}, nil)

	switch requestKeyVersion.ApiVersion {
	case 0:
		saslAuthReqV0 := &protocol.SaslAuthenticateRequestV0{}
		req := &protocol.Request{Body: saslAuthReqV0}
		if err = protocol.Decode(payload, req); err != nil {
			return err
		}

		authErr, userInfo := localSaslAuth.doLocalAuth(saslAuthReqV0.SaslAuthBytes)
		p.UserInfo = userInfo

		var saslAuthResV0 *protocol.SaslAuthenticateResponseV0
		if authErr == nil {
			// Length of SaslAuthBytes !=0 for OAUTHBEARER causes that java SaslClientAuthenticator in INTERMEDIATE state will sent SaslAuthenticate(36) second time
			saslAuthResV0 = &protocol.SaslAuthenticateResponseV0{Err: protocol.ErrNoError, SaslAuthBytes: make([]byte, 0)}
		} else {
			errMsg := authErr.Error()
			saslAuthResV0 = &protocol.SaslAuthenticateResponseV0{Err: protocol.ErrSASLAuthenticationFailed, ErrMsg: &errMsg, SaslAuthBytes: make([]byte, 0)}
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
	case 1:
		saslAuthReqV1 := &protocol.SaslAuthenticateRequestV1{}
		req := &protocol.Request{Body: saslAuthReqV1}
		if err = protocol.Decode(payload, req); err != nil {
			return err
		}

		authErr, userInfo := localSaslAuth.doLocalAuth(saslAuthReqV1.SaslAuthBytes)
		p.UserInfo = userInfo

		var saslAuthResV1 *protocol.SaslAuthenticateResponseV1
		if authErr == nil {
			// Length of SaslAuthBytes !=0 for OAUTHBEARER causes that java SaslClientAuthenticator in INTERMEDIATE state will sent SaslAuthenticate(36) second time
			saslAuthResV1 = &protocol.SaslAuthenticateResponseV1{Err: protocol.ErrNoError, SaslAuthBytes: make([]byte, 0), SessionLifetimeMs: 0}
		} else {
			errMsg := authErr.Error()
			saslAuthResV1 = &protocol.SaslAuthenticateResponseV1{Err: protocol.ErrSASLAuthenticationFailed, ErrMsg: &errMsg, SaslAuthBytes: make([]byte, 0), SessionLifetimeMs: 0}
		}
		newResponseBuf, err := protocol.Encode(saslAuthResV1)
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
	case 2:
		saslAuthReqV2 := &protocol.SaslAuthenticateRequestV2{}
		req := &protocol.RequestV2{Body: saslAuthReqV2}
		if err = protocol.Decode(payload, req); err != nil {
			return err
		}

		authErr, userInfo := localSaslAuth.doLocalAuth(saslAuthReqV2.SaslAuthBytes)
		p.UserInfo = userInfo

		var saslAuthResV2 *protocol.SaslAuthenticateResponseV2
		if authErr == nil {
			// Length of SaslAuthBytes !=0 for OAUTHBEARER causes that java SaslClientAuthenticator in INTERMEDIATE state will sent SaslAuthenticate(36) second time
			saslAuthResV2 = &protocol.SaslAuthenticateResponseV2{Err: protocol.ErrNoError, SaslAuthBytes: make([]byte, 0), SessionLifetimeMs: 0}
		} else {
			errMsg := authErr.Error()
			saslAuthResV2 = &protocol.SaslAuthenticateResponseV2{Err: protocol.ErrSASLAuthenticationFailed, ErrMsg: &errMsg, SaslAuthBytes: make([]byte, 0), SessionLifetimeMs: 0}
		}
		newResponseBuf, err := protocol.Encode(saslAuthResV2)
		if err != nil {
			return err
		}
		// 2 (Length) + 2 (CorrelationID) + 1 (empty TaggedFields)
		newHeaderBuf, err := protocol.Encode(&protocol.ResponseHeaderV1{Length: int32(len(newResponseBuf) + 5), CorrelationID: req.CorrelationID})
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
	default:
		return errors.Errorf("SaslAuthenticate version 0,1 or 2 is expected, apiVersion %d", requestKeyVersion.ApiVersion)
	}
}

func (p *LocalSasl) receiveAndSendAuthV0(conn DeadlineReaderWriter, localSaslAuth LocalSaslAuth) (err error) {
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

	if localSaslAuth == nil {
		return errors.New("localSaslAuth is nil")
	}

	err, userInfo := localSaslAuth.doLocalAuth(saslAuthBytes)

	if err != nil {
		return err
	}

	p.UserInfo = userInfo

	// If the credentials are valid, we would write a 4 byte response filled with null characters.
	// Otherwise, the closes the connection i.e. return error
	header := make([]byte, 4)
	if _, err := conn.Write(header); err != nil {
		return err
	}
	return nil
}
