package proxy

import (
	"errors"
	"fmt"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"github.com/sirupsen/logrus"
	"io"
	"strconv"
	"time"
)

type DefaultRequestHandler struct {
}

type DefaultResponseHandler struct {
}

func (handler *DefaultRequestHandler) handleRequest(dst DeadlineWriter, src DeadlineReaderWriter, ctx *RequestsLoopContext) (readErr bool, err error) {
	// logrus.Println("Await Kafka request")

	// waiting for first bytes or EOF - reset deadlines
	src.SetReadDeadline(time.Time{})
	dst.SetWriteDeadline(time.Time{})

	keyVersionBuf := make([]byte, 8) // Size => int32 + ApiKey => int16 + ApiVersion => int16

	if _, err = io.ReadFull(src, keyVersionBuf); err != nil {
		return true, err
	}

	requestKeyVersion := &protocol.RequestKeyVersion{}
	if err = protocol.Decode(keyVersionBuf, requestKeyVersion); err != nil {
		return true, err
	}
	logrus.Debugf("Kafka request key %v, version %v, length %v", requestKeyVersion.ApiKey, requestKeyVersion.ApiVersion, requestKeyVersion.Length)

	if requestKeyVersion.ApiKey < minRequestApiKey || requestKeyVersion.ApiKey > maxRequestApiKey {
		return true, fmt.Errorf("api key %d is invalid", requestKeyVersion.ApiKey)
	}

	proxyRequestsTotal.WithLabelValues(ctx.brokerAddress, strconv.Itoa(int(requestKeyVersion.ApiKey)), strconv.Itoa(int(requestKeyVersion.ApiVersion))).Inc()
	proxyRequestsBytes.WithLabelValues(ctx.brokerAddress).Add(float64(requestKeyVersion.Length + 4))

	if _, ok := ctx.forbiddenApiKeys[requestKeyVersion.ApiKey]; ok {
		return true, fmt.Errorf("api key %d is forbidden", requestKeyVersion.ApiKey)
	}

	if ctx.localSasl.enabled {
		if ctx.localSaslDone {
			if requestKeyVersion.ApiKey == apiKeySaslHandshake {
				return false, errors.New("SASL Auth was already done")
			}
		} else {
			switch requestKeyVersion.ApiKey {
			case apiKeySaslHandshake:
				switch requestKeyVersion.ApiVersion {
				case 0:
					if err = ctx.localSasl.receiveAndSendSASLAuthV0(src, keyVersionBuf); err != nil {
						return true, err
					}
				case 1:
					if err = ctx.localSasl.receiveAndSendSASLAuthV1(src, keyVersionBuf); err != nil {
						return true, err
					}
				default:
					return true, fmt.Errorf("only saslHandshake version 0 and 1 are supported, got version %d", requestKeyVersion.ApiVersion)
				}
				ctx.localSaslDone = true
				src.SetDeadline(time.Time{})

				// defaultRequestHandler was consumed but due to local handling enqueued defaultResponseHandler will not be.
				return false, ctx.putNextRequestHandler(defaultRequestHandler)
			case apiKeyApiApiVersions:
				// continue processing
			default:
				return false, errors.New("SASL Auth is required. Only SaslHandshake or ApiVersions requests are allowed")
			}
		}
	}

	// send inFlightRequest to channel before myCopyN to prevent race condition in proxyResponses
	if err = sendRequestKeyVersion(ctx.openRequestsChannel, openRequestSendTimeout, requestKeyVersion); err != nil {
		return true, err
	}

	requestDeadline := time.Now().Add(ctx.timeout)
	err = dst.SetWriteDeadline(requestDeadline)
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
	if readErr, err = myCopyN(dst, src, int64(requestKeyVersion.Length-4), ctx.buf); err != nil {
		return readErr, err
	}
	if requestKeyVersion.ApiKey == apiKeySaslHandshake {
		if requestKeyVersion.ApiVersion == 0 {
			return false, ctx.putNextHandlers(saslAuthV0RequestHandler, saslAuthV0ResponseHandler)
		}
	}
	return false, ctx.putNextHandlers(defaultRequestHandler, defaultResponseHandler)
}

func (handler *DefaultResponseHandler) handleResponse(dst DeadlineWriter, src DeadlineReader, ctx *ResponsesLoopContext) (readErr bool, err error) {
	//logrus.Println("Await Kafka response")

	// waiting for first bytes or EOF - reset deadlines
	src.SetReadDeadline(time.Time{})
	dst.SetWriteDeadline(time.Time{})

	responseHeaderBuf := make([]byte, 8) // Size => int32, CorrelationId => int32
	if _, err = io.ReadFull(src, responseHeaderBuf); err != nil {
		return true, err
	}

	var responseHeader protocol.ResponseHeader
	if err = protocol.Decode(responseHeaderBuf, &responseHeader); err != nil {
		return true, err
	}

	// Read the inFlightRequests channel after header is read. Otherwise the channel would block and socket EOF from remote would not be received.
	requestKeyVersion, err := receiveRequestKeyVersion(ctx.openRequestsChannel, openRequestReceiveTimeout)
	if err != nil {
		return true, err
	}
	proxyResponsesBytes.WithLabelValues(ctx.brokerAddress).Add(float64(responseHeader.Length + 4))
	logrus.Debugf("Kafka response key %v, version %v, length %v", requestKeyVersion.ApiKey, requestKeyVersion.ApiVersion, responseHeader.Length)

	responseDeadline := time.Now().Add(ctx.timeout)
	err = dst.SetWriteDeadline(responseDeadline)
	if err != nil {
		return false, err
	}
	err = src.SetReadDeadline(responseDeadline)
	if err != nil {
		return true, err
	}
	responseHeaderTaggedFields, err := protocol.NewResponseHeaderTaggedFields(requestKeyVersion)
	if err != nil {
		return true, err
	}
	unknownTaggedFields, err := responseHeaderTaggedFields.MaybeRead(src)
	if err != nil {
		return true, err
	}
	readResponsesHeaderLength := int32(4 + len(unknownTaggedFields)) // 4 = Length + CorrelationID

	responseModifier, err := protocol.GetResponseModifier(requestKeyVersion.ApiKey, requestKeyVersion.ApiVersion, ctx.netAddressMappingFunc)
	if err != nil {
		return true, err
	}
	if responseModifier != nil {
		if responseHeader.Length > protocol.MaxResponseSize {
			return true, protocol.PacketDecodingError{Info: fmt.Sprintf("message of length %d too large", responseHeader.Length)}
		}
		resp := make([]byte, int(responseHeader.Length-readResponsesHeaderLength))
		if _, err = io.ReadFull(src, resp); err != nil {
			return true, err
		}
		newResponseBuf, err := responseModifier.Apply(resp)
		if err != nil {
			return true, err
		}
		// add 4 bytes (CorrelationId) to the length
		newHeaderBuf, err := protocol.Encode(&protocol.ResponseHeader{Length: int32(len(newResponseBuf) + int(readResponsesHeaderLength)), CorrelationID: responseHeader.CorrelationID})
		if err != nil {
			return true, err
		}
		if _, err := dst.Write(newHeaderBuf); err != nil {
			return false, err
		}
		if _, err := dst.Write(unknownTaggedFields); err != nil {
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
		if _, err := dst.Write(unknownTaggedFields); err != nil {
			return false, err
		}
		// 4 bytes were written as responseHeaderBuf (CorrelationId) + tagged fields
		if readErr, err = myCopyN(dst, src, int64(responseHeader.Length-readResponsesHeaderLength), ctx.buf); err != nil {
			return readErr, err
		}
	}
	return false, nil // continue nextResponse
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
