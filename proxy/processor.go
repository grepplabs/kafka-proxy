package proxy

import (
	"errors"
	"fmt"
	"github.com/grepplabs/kafka-proxy/config"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"io"
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
}

type processor struct {
	openRequestsChannel   chan protocol.RequestKeyVersion
	netAddressMappingFunc config.NetAddressMappingFunc
	requestBufferSize     int
	responseBufferSize    int
	writeTimeout          time.Duration
	readTimeout           time.Duration
}

func newProcessor(cfg ProcessorConfig) *processor {
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
	}
}

func (p *processor) RequestsLoop(dst DeadlineWriter, src DeadlineReader) (readErr bool, err error) {
	return requestsLoop(dst, src, p.openRequestsChannel, p.requestBufferSize, p.writeTimeout)
}

func (p *processor) ResponsesLoop(dst DeadlineWriter, src DeadlineReader) (readErr bool, err error) {
	return responsesLoop(dst, src, p.openRequestsChannel, p.netAddressMappingFunc, p.responseBufferSize, p.readTimeout)
}

func requestsLoop(dst DeadlineWriter, src DeadlineReader, openRequestsChannel chan<- protocol.RequestKeyVersion, bufSize int, timeout time.Duration) (readErr bool, err error) {
	keyVersionBuf := make([]byte, 8) // Size => int32 + ApiKey => int16 + ApiVersion => int16

	buf := make([]byte, bufSize)

	for {
		// log.Println("Await Kafka request")

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
		// log.Printf("Kafka request length %v, key %v, version %v", requestKeyVersion.Length, requestKeyVersion.ApiKey, requestKeyVersion.ApiVersion)

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

func responsesLoop(dst DeadlineWriter, src DeadlineReader, openRequestsChannel <-chan protocol.RequestKeyVersion, netAddressMappingFunc config.NetAddressMappingFunc, bufSize int, timeout time.Duration) (readErr bool, err error) {
	responseHeaderBuf := make([]byte, 8) // Size => int32, CorrelationId => int32

	buf := make([]byte, bufSize)

	for {
		// log.Println("Await Kafka response")

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
		// log.Printf("Kafka response lenght %v for key %v, version %v", responseHeader.Length, requestKeyVersion.ApiKey, requestKeyVersion.ApiVersion)

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
