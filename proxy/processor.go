package proxy

import (
	"errors"
	"fmt"
	"github.com/grepplabs/kafka-proxy/config"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"io"
	"strconv"
	"time"
)

const (
	openRequestSendTimeout    = 5 * time.Second
	openRequestReceiveTimeout = 5 * time.Second
	defaultRequestBufferSize  = 4096
	defaultResponseBufferSize = 4096
	defaultWriteTimeout       = 30 * time.Second
	defaultReadTimeout        = 30 * time.Second
	minOpenRequests           = 16

	apiKeyUnset         = int16(-1) // not in protocol
	apiKeySaslAuth      = int16(-2) // not in protocol
	apiKeySaslHandshake = int16(17)

	minRequestApiKey = int16(0)   // 0 - Produce
	maxRequestApiKey = int16(100) // so far 42 is the last (reserve some for the feature)
)

var (
	defaultRequestHandler  = &DefaultRequestHandler{}
	defaultResponseHandler = &DefaultResponseHandler{}
)

type ProcessorConfig struct {
	MaxOpenRequests       int
	NetAddressMappingFunc config.NetAddressMappingFunc
	RequestBufferSize     int
	ResponseBufferSize    int
	WriteTimeout          time.Duration
	ReadTimeout           time.Duration
	LocalSasl             *LocalSasl
	AuthServer            *AuthServer
	ForbiddenApiKeys      map[int16]struct{}
}

type processor struct {
	openRequestsChannel        chan protocol.RequestKeyVersion
	nextRequestHandlerChannel  chan RequestHandler
	nextResponseHandlerChannel chan ResponseHandler

	netAddressMappingFunc config.NetAddressMappingFunc
	requestBufferSize     int
	responseBufferSize    int
	writeTimeout          time.Duration
	readTimeout           time.Duration

	localSasl  *LocalSasl
	authServer *AuthServer

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
	// in most use cases there will be only one entry in the nextRequestHandlerChannel channel
	nextRequestHandlerChannel := make(chan RequestHandler, minOpenRequests)
	nextResponseHandlerChannel := make(chan ResponseHandler, maxOpenRequests+1)

	// initial handlers -> standard kafka message arrives always as first
	nextRequestHandlerChannel <- defaultRequestHandler
	nextResponseHandlerChannel <- defaultResponseHandler

	return &processor{
		openRequestsChannel:        make(chan protocol.RequestKeyVersion, maxOpenRequests),
		nextRequestHandlerChannel:  nextRequestHandlerChannel,
		nextResponseHandlerChannel: nextResponseHandlerChannel,
		netAddressMappingFunc:      cfg.NetAddressMappingFunc,
		requestBufferSize:          requestBufferSize,
		responseBufferSize:         responseBufferSize,
		readTimeout:                readTimeout,
		writeTimeout:               writeTimeout,
		brokerAddress:              brokerAddress,
		localSasl:                  cfg.LocalSasl,
		authServer:                 cfg.AuthServer,
		forbiddenApiKeys:           cfg.ForbiddenApiKeys,
	}
}

func (p *processor) RequestsLoop(dst DeadlineWriter, src DeadlineReaderWriter) (readErr bool, err error) {

	if p.authServer.enabled {
		if err = p.authServer.receiveAndSendGatewayAuth(src); err != nil {
			return true, err
		}
	}
	if p.localSasl.enabled {
		if err = p.localSasl.receiveAndSendSASLPlainAuth(src); err != nil {
			return true, err
		}
	}
	src.SetDeadline(time.Time{})

	ctx := &RequestsLoopContext{
		openRequestsChannel:        p.openRequestsChannel,
		nextRequestHandlerChannel:  p.nextRequestHandlerChannel,
		nextResponseHandlerChannel: p.nextResponseHandlerChannel,
		timeout:                    p.writeTimeout,
		brokerAddress:              p.brokerAddress,
		forbiddenApiKeys:           p.forbiddenApiKeys,
		keyVersionBuf:              make([]byte, 8), // Size => int32 + ApiKey => int16 + ApiVersion => int16
		buf:                        make([]byte, p.requestBufferSize),
		lastApiKey:                 apiKeyUnset,
	}

	return ctx.requestsLoop(dst, src)
}

type RequestsLoopContext struct {
	openRequestsChannel        chan<- protocol.RequestKeyVersion
	nextRequestHandlerChannel  chan RequestHandler
	nextResponseHandlerChannel chan<- ResponseHandler

	timeout          time.Duration
	brokerAddress    string
	forbiddenApiKeys map[int16]struct{}

	// bufSize int
	keyVersionBuf []byte // 8 Size => int32 + ApiKey => int16 + ApiVersion => int16
	buf           []byte // bufSize
	lastApiKey    int16
}

func (ctx *RequestsLoopContext) nextHandlers(nextRequestHandler RequestHandler, nextResponseHandler ResponseHandler) error {

	select {
	case ctx.nextRequestHandlerChannel <- nextRequestHandler:
	default:
		return errors.New("next request handler channel is full")
	}

	select {
	case ctx.nextResponseHandlerChannel <- nextResponseHandler:
	default:
		// timer.Stop() will be invoked only after nextHandlers is finished
		timer := time.NewTimer(openRequestReceiveTimeout) // reuse openRequestReceiveTimeout
		defer timer.Stop()

		select {
		case ctx.nextResponseHandlerChannel <- nextResponseHandler:
		case <-timer.C:
			return errors.New("next response handler channel is full")
		}
	}
	return nil
}

type RequestHandler interface {
	handleRequest(dst DeadlineWriter, src DeadlineReader, ctx *RequestsLoopContext) (readErr bool, err error)
}

func (r *RequestsLoopContext) requestsLoop(dst DeadlineWriter, src DeadlineReader) (readErr bool, err error) {
	for {
		select {
		case nextRequestHandler := <-r.nextRequestHandlerChannel:
			if readErr, err = nextRequestHandler.handleRequest(dst, src, r); err != nil {
				return readErr, err
			}
		default:
			return false, errors.New("internal error: next request handler expected")
		}
	}
}

type DefaultRequestHandler struct {
}

func (handler *DefaultRequestHandler) handleRequest(dst DeadlineWriter, src DeadlineReader, ctx *RequestsLoopContext) (readErr bool, err error) {
	if ctx.lastApiKey == apiKeySaslHandshake {
		ctx.lastApiKey = apiKeySaslAuth
		if readErr, err = copySaslAuthRequest(dst, src, ctx.timeout, ctx.buf); err != nil {
			return readErr, err
		}
		if err = ctx.nextHandlers(defaultRequestHandler, defaultResponseHandler); err != nil {
			return false, err
		}
		return false, nil
	}
	if len(ctx.keyVersionBuf) != 8 {
		return false, errors.New("key version buf should have size 8")
	}
	// logrus.Println("Await Kafka request")

	// waiting for first bytes or EOF - reset deadlines
	src.SetReadDeadline(time.Time{})
	dst.SetWriteDeadline(time.Time{})

	if _, err = io.ReadFull(src, ctx.keyVersionBuf); err != nil {
		return true, err
	}

	requestKeyVersion := &protocol.RequestKeyVersion{}
	if err = protocol.Decode(ctx.keyVersionBuf, requestKeyVersion); err != nil {
		return true, err
	}
	//logrus.Printf("Kafka request length %v, key %v, version %v", requestKeyVersion.Length, requestKeyVersion.ApiKey, requestKeyVersion.ApiVersion)

	if requestKeyVersion.ApiKey < minRequestApiKey || requestKeyVersion.ApiKey > maxRequestApiKey {
		return true, fmt.Errorf("api key %d is invalid", requestKeyVersion.ApiKey)
	}

	proxyRequestsTotal.WithLabelValues(ctx.brokerAddress, strconv.Itoa(int(requestKeyVersion.ApiKey)), strconv.Itoa(int(requestKeyVersion.ApiVersion))).Inc()
	proxyRequestsBytes.WithLabelValues(ctx.brokerAddress).Add(float64(requestKeyVersion.Length + 4))

	if _, ok := ctx.forbiddenApiKeys[requestKeyVersion.ApiKey]; ok {
		return true, fmt.Errorf("api key %d is forbidden", requestKeyVersion.ApiKey)
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
	if _, err = dst.Write(ctx.keyVersionBuf); err != nil {
		return false, err
	}
	// 4 bytes were written as keyVersionBuf (ApiKey, ApiVersion)
	if readErr, err = myCopyN(dst, src, int64(requestKeyVersion.Length-4), ctx.buf); err != nil {
		return readErr, err
	}

	ctx.lastApiKey = requestKeyVersion.ApiKey

	if err = ctx.nextHandlers(defaultRequestHandler, defaultResponseHandler); err != nil {
		return false, err
	}
	return false, nil
}

func (p *processor) ResponsesLoop(dst DeadlineWriter, src DeadlineReader) (readErr bool, err error) {
	ctx := &ResponsesLoopContext{
		openRequestsChannel:        p.openRequestsChannel,
		nextResponseHandlerChannel: p.nextResponseHandlerChannel,
		netAddressMappingFunc:      p.netAddressMappingFunc,
		timeout:                    p.readTimeout,
		brokerAddress:              p.brokerAddress,

		responseHeaderBuf: make([]byte, 8), // Size => int32, CorrelationId => int32
		buf:               make([]byte, p.responseBufferSize),
		lastApiKey:        apiKeyUnset,
	}
	return ctx.responsesLoop(dst, src)
}

type ResponsesLoopContext struct {
	openRequestsChannel        <-chan protocol.RequestKeyVersion
	nextResponseHandlerChannel <-chan ResponseHandler
	netAddressMappingFunc      config.NetAddressMappingFunc
	timeout                    time.Duration
	brokerAddress              string

	responseHeaderBuf []byte // 8 - Size => int32, CorrelationId => int32
	buf               []byte // bufSize
	lastApiKey        int16
}

type ResponseHandler interface {
	handleResponse(dst DeadlineWriter, src DeadlineReader, ctx *ResponsesLoopContext) (readErr bool, err error)
}

func (r *ResponsesLoopContext) responsesLoop(dst DeadlineWriter, src DeadlineReader) (readErr bool, err error) {
	for {
		//TODO: timeout noting was received
		nextResponseHandler := <-r.nextResponseHandlerChannel
		if readErr, err = nextResponseHandler.handleResponse(dst, src, r); err != nil {
			return readErr, err
		}
	}
}

type DefaultResponseHandler struct {
}

func (handler *DefaultResponseHandler) handleResponse(dst DeadlineWriter, src DeadlineReader, ctx *ResponsesLoopContext) (readErr bool, err error) {
	if ctx.lastApiKey == apiKeySaslHandshake {
		ctx.lastApiKey = apiKeySaslAuth
		if readErr, err = copySaslAuthResponse(dst, src, ctx.timeout); err != nil {
			return readErr, err
		}
		return false, nil // nextResponse
	}
	//logrus.Println("Await Kafka response")

	// waiting for first bytes or EOF - reset deadlines
	src.SetReadDeadline(time.Time{})
	dst.SetWriteDeadline(time.Time{})

	if len(ctx.responseHeaderBuf) != 8 {
		return false, errors.New("response header buf should have size 8")
	}

	if _, err = io.ReadFull(src, ctx.responseHeaderBuf); err != nil {
		return true, err
	}

	var responseHeader protocol.ResponseHeader
	if err = protocol.Decode(ctx.responseHeaderBuf, &responseHeader); err != nil {
		return true, err
	}

	// Read the inFlightRequests channel after header is read. Otherwise the channel would block and socket EOF from remote would not be received.
	requestKeyVersion, err := receiveRequestKeyVersion(ctx.openRequestsChannel, openRequestReceiveTimeout)
	if err != nil {
		return true, err
	}
	proxyResponsesBytes.WithLabelValues(ctx.brokerAddress).Add(float64(responseHeader.Length + 4))
	//logrus.Printf("Kafka response lenght %v for key %v, version %v", responseHeader.Length, requestKeyVersion.ApiKey, requestKeyVersion.ApiVersion)

	responseDeadline := time.Now().Add(ctx.timeout)
	err = dst.SetWriteDeadline(responseDeadline)
	if err != nil {
		return false, err
	}
	err = src.SetReadDeadline(responseDeadline)
	if err != nil {
		return true, err
	}

	responseModifier, err := protocol.GetResponseModifier(requestKeyVersion.ApiKey, requestKeyVersion.ApiVersion, ctx.netAddressMappingFunc)
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
		if _, err := dst.Write(ctx.responseHeaderBuf); err != nil {
			return false, err
		}
		// 4 bytes were written as responseHeaderBuf (CorrelationId)
		if readErr, err = myCopyN(dst, src, int64(responseHeader.Length-4), ctx.buf); err != nil {
			return readErr, err
		}
	}
	ctx.lastApiKey = requestKeyVersion.ApiKey

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
