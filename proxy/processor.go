package proxy

import (
	"errors"
	"github.com/grepplabs/kafka-proxy/config"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
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

	apiKeySaslHandshake = int16(17)

	minRequestApiKey = int16(0)   // 0 - Produce
	maxRequestApiKey = int16(100) // so far 42 is the last (reserve some for the feature)
)

var (
	defaultRequestHandler     = &DefaultRequestHandler{}
	defaultResponseHandler    = &DefaultResponseHandler{}
	saslAuthV0RequestHandler  = &SaslAuthV0RequestHandler{}
	saslAuthV0ResponseHandler = &SaslAuthV0ResponseHandler{}
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
		//TODO: when localSasl is enabled SASL is mandadory - we need authorized e.g. in RequestsLoopContext (mutex ?)
		//TODO: before SASL only ApiVersions is required
		//TODO: SASL can be done only once
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
		buf:                        make([]byte, p.requestBufferSize),
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
	buf              []byte // bufSize
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

func (p *processor) ResponsesLoop(dst DeadlineWriter, src DeadlineReader) (readErr bool, err error) {
	ctx := &ResponsesLoopContext{
		openRequestsChannel:        p.openRequestsChannel,
		nextResponseHandlerChannel: p.nextResponseHandlerChannel,
		netAddressMappingFunc:      p.netAddressMappingFunc,
		timeout:                    p.readTimeout,
		brokerAddress:              p.brokerAddress,
		buf:                        make([]byte, p.responseBufferSize),
	}
	return ctx.responsesLoop(dst, src)
}

type ResponsesLoopContext struct {
	openRequestsChannel        <-chan protocol.RequestKeyVersion
	nextResponseHandlerChannel <-chan ResponseHandler
	netAddressMappingFunc      config.NetAddressMappingFunc
	timeout                    time.Duration
	brokerAddress              string
	buf                        []byte // bufSize
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
