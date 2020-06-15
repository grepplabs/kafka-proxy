package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"github.com/sirupsen/logrus"
)

type DefaultRequestHandler struct {
}

type DefaultResponseHandler struct {
}

func (handler *DefaultRequestHandler) handleRequest(dst DeadlineWriter, src DeadlineReaderWriter, ctx *RequestsLoopContext) (readErr bool, err error) {
	// logrus.Println("Await Kafka request")

	// waiting for first bytes or EOF - reset deadlines
	var payload []byte = nil
	if err = src.SetReadDeadline(time.Time{}); err != nil {
		return true, err
	}
	if err = dst.SetWriteDeadline(time.Time{}); err != nil {
		return true, err
	}

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

			if ctx.authz.enabled {
				authzRequest := apis.AuthzRequest{
					UserInfo:   ctx.localSasl.UserInfo,
					Apikey:     int32(requestKeyVersion.ApiKey),
					Apiversion: int32(requestKeyVersion.ApiVersion),
					DstIp:      ctx.brokerAddress,
					SrcIp:      ctx.srcAddress,
				}

				decodedApiKeys := map[int16]bool{
					0:  true,
					1:  true,
					2:  true,
					3:  true,
					8:  true,
					9:  true,
					19: true,
					20: true,
					37: true,
				}

				if _, ok := decodedApiKeys[requestKeyVersion.ApiKey]; ok {
					req, reqBody, pay, err := getPartialDecodedRequest(keyVersionBuf, src)
					payload = pay
					topics := reqBody.(protocol.TopicRequestInterface).GetTopics()

					if err != nil {
						return false, err
					}

					authzRequest.Topics = strings.Join(topics, ";")
					authzRequest.ClientId = req.ClientID
				}

				authResponse, err := ctx.authz.authzProvider.Authorize(context.Background(), authzRequest)

				if err != nil {
					return false, fmt.Errorf("Problem when calling authorization: %v", err)
				}

				if !authResponse.Success {
					err := fmt.Errorf(
						"Authorization failed status: %d, apiversion: %d, apikey: %d, dstip: %s, srcip: %s, topics: %s, clientId: %s",
						authResponse.Status,
						int32(requestKeyVersion.ApiVersion),
						int32(requestKeyVersion.ApiKey),
						ctx.brokerAddress,
						ctx.srcAddress,
						authzRequest.Topics,
						authzRequest.ClientId,
					)
					return false, err
				}
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
				if err = src.SetDeadline(time.Time{}); err != nil {
					return false, err
				}
				// defaultRequestHandler was consumed but due to local handling enqueued defaultResponseHandler will not be.
				return false, ctx.putNextRequestHandler(defaultRequestHandler)
			case apiKeyApiApiVersions:
				// continue processing
			default:
				return false, errors.New("SASL Auth is required. Only SaslHandshake or ApiVersions requests are allowed")
			}
		}
	}

	mustReply, _, err := handler.mustReply(requestKeyVersion, src, ctx)
	if err != nil {
		return true, err
	}

	// send inFlightRequest to channel before myCopyN to prevent race condition in proxyResponses
	if mustReply {
		if err = sendRequestKeyVersion(ctx.openRequestsChannel, openRequestSendTimeout, requestKeyVersion); err != nil {
			return true, err
		}
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

	if payload != nil {
		if _, err = dst.Write(payload); err != nil {
			return false, err
		}
	} else {
		// 4 bytes were written as keyVersionBuf (ApiKey, ApiVersion)
		if readErr, err = myCopyN(dst, src, int64(requestKeyVersion.Length-4), ctx.buf); err != nil {
			return readErr, err
		}
	}

	if requestKeyVersion.ApiKey == apiKeySaslHandshake {
		if requestKeyVersion.ApiVersion == 0 {
			return false, ctx.putNextHandlers(saslAuthV0RequestHandler, saslAuthV0ResponseHandler)
		}
	}
	if mustReply {
		return false, ctx.putNextHandlers(defaultRequestHandler, defaultResponseHandler)
	} else {
		return false, ctx.putNextRequestHandler(defaultRequestHandler)
	}
}

func (handler *DefaultRequestHandler) mustReply(requestKeyVersion *protocol.RequestKeyVersion, src io.Reader, ctx *RequestsLoopContext) (bool, []byte, error) {
	if requestKeyVersion.ApiKey == apiKeyProduce {
		if ctx.producerAcks0Disabled {
			return true, nil, nil
		}
		// header version for produce [0..8] is 1 (request_api_key,request_api_version,correlation_id (INT32),client_id, NULLABLE_STRING )
		acksReader := protocol.RequestAcksReader{}

		var (
			acks int16
			err  error
		)
		var bufferRead bytes.Buffer
		reader := io.TeeReader(src, &bufferRead)
		switch requestKeyVersion.ApiVersion {
		case 0, 1, 2:
			// CorrelationID + ClientID
			if err = acksReader.ReadAndDiscardHeaderV1Part(reader); err != nil {
				return false, nil, err
			}
			// acks (INT16)
			acks, err = acksReader.ReadAndDiscardProduceAcks(reader)
			if err != nil {
				return false, nil, err
			}

		case 3, 4, 5, 6, 7, 8:
			// CorrelationID + ClientID
			if err = acksReader.ReadAndDiscardHeaderV1Part(reader); err != nil {
				return false, nil, err
			}
			// transactional_id (NULLABLE_STRING),acks (INT16)
			acks, err = acksReader.ReadAndDiscardProduceTxnAcks(reader)
			if err != nil {
				return false, nil, err
			}
		default:
			return false, nil, fmt.Errorf("produce version %d is not supported", requestKeyVersion.ApiVersion)
		}
		return acks != 0, bufferRead.Bytes(), nil
	}
	return true, nil, nil
}

func (handler *DefaultResponseHandler) handleResponse(dst DeadlineWriter, src DeadlineReader, ctx *ResponsesLoopContext) (readErr bool, err error) {
	//logrus.Println("Await Kafka response")

	// waiting for first bytes or EOF - reset deadlines
	if err = src.SetReadDeadline(time.Time{}); err != nil {
		return true, err
	}
	if err = dst.SetWriteDeadline(time.Time{}); err != nil {
		return true, err
	}

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
