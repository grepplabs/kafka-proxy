package proxy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/grepplabs/kafka-proxy/config"
	"github.com/grepplabs/kafka-proxy/plugin/local-auth/shared"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"io"
	"strconv"
	"strings"
	"time"
)

const (
	openRequestSendTimeout    = 5 * time.Second
	openRequestReceiveTimeout = 5 * time.Second
	defaultRequestBufferSize  = 4096
	defaultResponseBufferSize = 4096
	defaultWriteTimeout       = 30 * time.Second
	defaultReadTimeout        = 30 * time.Second
	minOpenRequests           = 1
)

type ProcessorConfig struct {
	MaxOpenRequests       int
	NetAddressMappingFunc config.NetAddressMappingFunc
	RequestBufferSize     int
	ResponseBufferSize    int
	WriteTimeout          time.Duration
	ReadTimeout           time.Duration
	LocalAuth             bool
	LocalAuthenticator    shared.PasswordAuthenticator
	ForbiddenApiKeys      map[int16]struct{}
}

type processor struct {
	openRequestsChannel   chan protocol.RequestKeyVersion
	netAddressMappingFunc config.NetAddressMappingFunc
	requestBufferSize     int
	responseBufferSize    int
	writeTimeout          time.Duration
	readTimeout           time.Duration

	localAuth          bool
	localAuthenticator shared.PasswordAuthenticator

	forbiddenApiKeys map[int16]struct{}
	// metrics
	brokerAddress string
}

func newProcessor(cfg ProcessorConfig, brokerAddress string) *processor {
	maxOpenRequests := cfg.MaxOpenRequests
	if maxOpenRequests < minOpenRequests {
		maxOpenRequests = minOpenRequests
	}
	requestBufferSize := cfg.RequestBufferSize
	if requestBufferSize <= 0 {
		requestBufferSize = defaultRequestBufferSize
	}
	responseBufferSize := cfg.ResponseBufferSize
	if responseBufferSize <= 0 {
		responseBufferSize = defaultResponseBufferSize
	}
	writeTimeout := cfg.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = defaultWriteTimeout
	}
	readTimeout := cfg.ReadTimeout
	if readTimeout <= 0 {
		readTimeout = defaultReadTimeout
	}
	return &processor{
		openRequestsChannel:   make(chan protocol.RequestKeyVersion, maxOpenRequests),
		netAddressMappingFunc: cfg.NetAddressMappingFunc,
		requestBufferSize:     requestBufferSize,
		responseBufferSize:    responseBufferSize,
		readTimeout:           readTimeout,
		writeTimeout:          writeTimeout,
		brokerAddress:         brokerAddress,
		localAuth:             cfg.LocalAuth,
		localAuthenticator:    cfg.LocalAuthenticator,
		forbiddenApiKeys:      cfg.ForbiddenApiKeys,
	}
}
func (p *processor) doLocalSasl(dst DeadlineWriter, src DeadlineReader) (err error) {
	requestDeadline := time.Now().Add(p.readTimeout)
	err = dst.SetWriteDeadline(requestDeadline)
	if err != nil {
		return err
	}
	err = src.SetReadDeadline(requestDeadline)
	if err != nil {
		return err
	}

	keyVersionBuf := make([]byte, 8) // Size => int32 + ApiKey => int16 + ApiVersion => int16
	if _, err = io.ReadFull(src, keyVersionBuf); err != nil {
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
	if _, err = io.ReadFull(src, resp); err != nil {
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
	if _, err := dst.Write(newHeaderBuf); err != nil {
		return err
	}
	if _, err := dst.Write(newResponseBuf); err != nil {
		return err
	}
	return saslResult
}

func (p *processor) doLocalAuth(dst DeadlineWriter, src DeadlineReader) (err error) {
	requestDeadline := time.Now().Add(p.readTimeout)
	err = dst.SetWriteDeadline(requestDeadline)
	if err != nil {
		return err
	}
	err = src.SetReadDeadline(requestDeadline)
	if err != nil {
		return err
	}

	sizeBuf := make([]byte, 4) // Size => int32
	if _, err = io.ReadFull(src, sizeBuf); err != nil {
		return err
	}

	length := binary.BigEndian.Uint32(sizeBuf)
	if int32(length) > protocol.MaxRequestSize {
		return protocol.PacketDecodingError{Info: fmt.Sprintf("auth message of length %d too large", length)}
	}

	payload := make([]byte, length)
	_, err = io.ReadFull(src, payload)
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
	if _, err := dst.Write(header); err != nil {
		return err
	}
	return nil
}

func (p *processor) RequestsLoop(dst DeadlineWriter, src DeadlineReaderWriter) (readErr bool, err error) {

	if p.localAuth {
		if err = p.doLocalSasl(src, src); err != nil {
			return true, err
		}
		if err = p.doLocalAuth(src, src); err != nil {
			return true, err
		}
	}
	src.SetReadDeadline(time.Time{})
	src.SetWriteDeadline(time.Time{})

	return requestsLoop(dst, src, p.openRequestsChannel, p.requestBufferSize, p.writeTimeout, p.brokerAddress, p.forbiddenApiKeys)
}

func (p *processor) ResponsesLoop(dst DeadlineWriter, src DeadlineReader) (readErr bool, err error) {
	return responsesLoop(dst, src, p.openRequestsChannel, p.netAddressMappingFunc, p.responseBufferSize, p.readTimeout, p.brokerAddress)
}

func requestsLoop(dst DeadlineWriter, src DeadlineReader, openRequestsChannel chan<- protocol.RequestKeyVersion, bufSize int, timeout time.Duration, brokerAddress string, forbiddenApiKeys map[int16]struct{}) (readErr bool, err error) {
	keyVersionBuf := make([]byte, 8) // Size => int32 + ApiKey => int16 + ApiVersion => int16

	buf := make([]byte, bufSize)

	for {
		// logrus.Println("Await Kafka request")

		// waiting for first bytes or EOF - reset deadlines
		src.SetReadDeadline(time.Time{})
		dst.SetWriteDeadline(time.Time{})

		if _, err = io.ReadFull(src, keyVersionBuf); err != nil {
			return true, err
		}

		requestKeyVersion := &protocol.RequestKeyVersion{}
		if err = protocol.Decode(keyVersionBuf, requestKeyVersion); err != nil {
			return true, err
		}
		// logrus.Printf("Kafka request length %v, key %v, version %v", requestKeyVersion.Length, requestKeyVersion.ApiKey, requestKeyVersion.ApiVersion)

		proxyRequestsTotal.WithLabelValues(brokerAddress, strconv.Itoa(int(requestKeyVersion.ApiKey)), strconv.Itoa(int(requestKeyVersion.ApiVersion))).Inc()
		proxyRequestsBytes.WithLabelValues(brokerAddress).Add(float64(requestKeyVersion.Length + 4))

		if _, ok := forbiddenApiKeys[requestKeyVersion.ApiKey]; ok {
			return true, fmt.Errorf("api key %d is forbidden", requestKeyVersion.ApiKey)
		}

		// send inFlightRequest to channel before myCopyN to prevent race condition in proxyResponses
		if err = sendRequestKeyVersion(openRequestsChannel, openRequestSendTimeout, requestKeyVersion); err != nil {
			return true, err
		}

		requestDeadline := time.Now().Add(timeout)
		err := dst.SetWriteDeadline(requestDeadline)
		if err != nil {
			return false, err
		}
		err = src.SetReadDeadline(requestDeadline)
		if err != nil {
			return true, err
		}

		// write - send to broker
		if _, err = dst.Write(keyVersionBuf); err != nil {
			return false, err
		}
		// 4 bytes were written as keyVersionBuf (ApiKey, ApiVersion)
		if readErr, err = myCopyN(dst, src, int64(requestKeyVersion.Length-4), buf); err != nil {
			return readErr, err
		}
	}
}

func responsesLoop(dst DeadlineWriter, src DeadlineReader, openRequestsChannel <-chan protocol.RequestKeyVersion, netAddressMappingFunc config.NetAddressMappingFunc, bufSize int, timeout time.Duration, brokerAddress string) (readErr bool, err error) {
	responseHeaderBuf := make([]byte, 8) // Size => int32, CorrelationId => int32

	buf := make([]byte, bufSize)

	for {
		//logrus.Println("Await Kafka response")

		// waiting for first bytes or EOF - reset deadlines
		src.SetReadDeadline(time.Time{})
		dst.SetWriteDeadline(time.Time{})

		if _, err = io.ReadFull(src, responseHeaderBuf); err != nil {
			return true, err
		}

		var responseHeader protocol.ResponseHeader
		if err = protocol.Decode(responseHeaderBuf, &responseHeader); err != nil {
			return true, err
		}

		// Read the inFlightRequests channel after header is read. Otherwise the channel would block and socket EOF from remote would not be received.
		requestKeyVersion, err := receiveRequestKeyVersion(openRequestsChannel, openRequestReceiveTimeout)
		if err != nil {
			return true, err
		}
		proxyResponsesBytes.WithLabelValues(brokerAddress).Add(float64(responseHeader.Length + 4))
		//logrus.Printf("Kafka response lenght %v for key %v, version %v", responseHeader.Length, requestKeyVersion.ApiKey, requestKeyVersion.ApiVersion)

		responseDeadline := time.Now().Add(timeout)
		err = dst.SetWriteDeadline(responseDeadline)
		if err != nil {
			return false, err
		}
		err = src.SetReadDeadline(responseDeadline)
		if err != nil {
			return true, err
		}

		responseModifier, err := protocol.GetResponseModifier(requestKeyVersion.ApiKey, requestKeyVersion.ApiVersion, netAddressMappingFunc)
		if err != nil {
			return true, err
		}
		if responseModifier != nil {
			if int32(responseHeader.Length) > protocol.MaxResponseSize {
				return true, protocol.PacketDecodingError{Info: fmt.Sprintf("message of length %d too large", responseHeader.Length)}
			}
			resp := make([]byte, int(responseHeader.Length-4))
			if _, err = io.ReadFull(src, resp); err != nil {
				return true, err
			}
			newResponseBuf, err := responseModifier.Apply(resp)
			if err != nil {
				return true, err
			}
			// add 4 bytes (CorrelationId) to the length
			newHeaderBuf, err := protocol.Encode(&protocol.ResponseHeader{Length: int32(len(newResponseBuf) + 4), CorrelationID: responseHeader.CorrelationID})
			if err != nil {
				return true, err
			}
			if _, err := dst.Write(newHeaderBuf); err != nil {
				return false, err
			}
			if _, err := dst.Write(newResponseBuf); err != nil {
				return false, err
			}
		} else {
			// write - send to local
			if _, err := dst.Write(responseHeaderBuf); err != nil {
				return false, err
			}
			// 4 bytes were written as responseHeaderBuf (CorrelationId)
			if readErr, err = myCopyN(dst, src, int64(responseHeader.Length-4), buf); err != nil {
				return readErr, err
			}
		}
	}
}

func sendRequestKeyVersion(openRequestsChannel chan<- protocol.RequestKeyVersion, timeout time.Duration, request *protocol.RequestKeyVersion) error {
	select {
	case openRequestsChannel <- *request:
	default:
		// timer.Stop() will be invoked only after sendRequestKeyVersion is finished (not after select default) !
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case openRequestsChannel <- *request:
		case <-timer.C:
			return errors.New("open requests buffer is full")
		}
	}
	return nil
}

func receiveRequestKeyVersion(openRequestsChannel <-chan protocol.RequestKeyVersion, timeout time.Duration) (*protocol.RequestKeyVersion, error) {
	var request protocol.RequestKeyVersion
	select {
	case request = <-openRequestsChannel:
	default:
		// timer.Stop() will be invoked only after receiveRequestKeyVersion is finished (not after select default) !
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case request = <-openRequestsChannel:
		case <-timer.C:
			return nil, errors.New("open request is missing")
		}
	}
	return &request, nil
}
