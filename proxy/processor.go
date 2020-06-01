package proxy

import (
	"errors"
	"time"

	"github.com/grepplabs/kafka-proxy/config"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
)

const (
	openRequestSendTimeout    = 5 * time.Second
	openRequestReceiveTimeout = 5 * time.Second
	defaultRequestBufferSize  = 4096
	defaultResponseBufferSize = 4096
	defaultWriteTimeout       = 30 * time.Second
	defaultReadTimeout        = 30 * time.Second
	minOpenRequests           = 16

	apiKeyProduce        = int16(0)
	apiKeySaslHandshake  = int16(17)
	apiKeyApiApiVersions = int16(18)

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
	Authz                 *Authz
	LocalSasl             *LocalSasl
	AuthServer            *AuthServer
	ForbiddenApiKeys      map[int16]struct{}
	ProducerAcks0Disabled bool
	SrcAddress            string
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

	authz      *Authz
	localSasl  *LocalSasl
	authServer *AuthServer

	forbiddenApiKeys map[int16]struct{}
	// metrics
	brokerAddress string
	// producer will never send request with acks=0
	producerAcks0Disabled bool
	srcAddress    string
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
	nextRequestHandlerChannel := make(chan RequestHandler, 1)
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
		srcAddress:                 cfg.SrcAddress,
		authz:                      cfg.Authz,
		localSasl:                  cfg.LocalSasl,
		authServer:                 cfg.AuthServer,
		forbiddenApiKeys:           cfg.ForbiddenApiKeys,
		producerAcks0Disabled:      cfg.ProducerAcks0Disabled,
	}
}

func (p *processor) RequestsLoop(dst DeadlineWriter, src DeadlineReaderWriter) (readErr bool, err error) {

	if p.authServer.enabled {
		if err = p.authServer.receiveAndSendGatewayAuth(src); err != nil {
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
		srcAddress:                 p.srcAddress,
		forbiddenApiKeys:           p.forbiddenApiKeys,
		buf:                        make([]byte, p.requestBufferSize),
		authz:                      p.authz,
		localSasl:                  p.localSasl,
		localSaslDone:              false, // sequential processing - mutex is required
		producerAcks0Disabled:      p.producerAcks0Disabled,
	}

	return ctx.requestsLoop(dst, src)
}

type RequestsLoopContext struct {
	openRequestsChannel        chan<- protocol.RequestKeyVersion
	nextRequestHandlerChannel  chan RequestHandler
	nextResponseHandlerChannel chan<- ResponseHandler

	timeout          time.Duration
	brokerAddress    string
	srcAddress       string
	forbiddenApiKeys map[int16]struct{}
	buf              []byte // bufSize

	authz         *Authz
	localSasl     *LocalSasl
	localSaslDone bool

	producerAcks0Disabled bool
}

// used by local authentication
func (ctx *RequestsLoopContext) putNextRequestHandler(nextRequestHandler RequestHandler) error {

	select {
	case ctx.nextRequestHandlerChannel <- nextRequestHandler:
	default:
		return errors.New("next request handler channel is full")
	}
	return nil
}

func (ctx *RequestsLoopContext) putNextHandlers(nextRequestHandler RequestHandler, nextResponseHandler ResponseHandler) error {

	select {
	case ctx.nextRequestHandlerChannel <- nextRequestHandler:
	default:
		return errors.New("next request handler channel is full")
	}

	select {
	case ctx.nextResponseHandlerChannel <- nextResponseHandler:
	default:
		timer := time.NewTimer(openRequestSendTimeout)
		defer timer.Stop()

		select {
		case ctx.nextResponseHandlerChannel <- nextResponseHandler:
		case <-timer.C:
			return errors.New("next response handler channel is full")
		}
	}
	return nil
}

func (r *RequestsLoopContext) getNextRequestHandler() (RequestHandler, error) {
	select {
	case nextRequestHandler := <-r.nextRequestHandlerChannel:
		return nextRequestHandler, nil
	default:
		return nil, errors.New("next request handler is missing")
	}
}

type RequestHandler interface {
	handleRequest(dst DeadlineWriter, src DeadlineReaderWriter, ctx *RequestsLoopContext) (readErr bool, err error)
}

func (r *RequestsLoopContext) requestsLoop(dst DeadlineWriter, src DeadlineReaderWriter) (readErr bool, err error) {
	var nextRequestHandler RequestHandler
	for {
		if nextRequestHandler, err = r.getNextRequestHandler(); err != nil {
			return false, nil
		}
		if readErr, err = nextRequestHandler.handleRequest(dst, src, r); err != nil {
			return readErr, err
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
	var nextResponseHandler ResponseHandler
	for {
		if nextResponseHandler, err = r.getNextResponseHandler(); err != nil {
			return false, err
		}
		if readErr, err = nextResponseHandler.handleResponse(dst, src, r); err != nil {
			return readErr, err
		}
	}
}

func (r *ResponsesLoopContext) getNextResponseHandler() (ResponseHandler, error) {
	select {
	case handler := <-r.nextResponseHandlerChannel:
		return handler, nil
	default:
		timer := time.NewTimer(openRequestReceiveTimeout)
		defer timer.Stop()

		select {
		case handler := <-r.nextResponseHandlerChannel:
			return handler, nil
		case <-timer.C:
			return nil, errors.New("next response handler is missing")
		}
	}
}
