package proxy

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
)

type Authz struct {
	authzProvider apis.AuthzProvider
	enabled       bool
}

func getPartialDecodedRequest(keyVersionBuf []byte, src DeadlineReaderWriter) (req *protocol.Request, reqBody protocol.ProtocolBody, pay []byte, err error) {
	if len(keyVersionBuf) != 8 {
		return nil, nil, nil, errors.New("length of keyVersionBuf should be 8")
	}
	// keyVersionBuf has already been read from connection
	requestKeyVersion := &protocol.RequestKeyVersion{}
	if err = protocol.Decode(keyVersionBuf, requestKeyVersion); err != nil {
		return nil, nil, nil, err
	}

	if int32(requestKeyVersion.Length) > protocol.MaxRequestSize {
		return nil, nil, nil, protocol.PacketDecodingError{Info: fmt.Sprintf("message of length %d too large", requestKeyVersion.Length)}
	}

	resp := make([]byte, int(requestKeyVersion.Length-4))
	if _, err = io.ReadFull(src, resp); err != nil {
		return nil, nil, nil, err
	}

	payload := bytes.Join([][]byte{keyVersionBuf[4:], resp}, nil)

	factory := &protocol.RequestTypeFactory{}
	typeReq, err := factory.Produce(requestKeyVersion)

	if err != nil {
		return nil, nil, nil, err
	}

	//
	req = &protocol.Request{Body: typeReq.(protocol.ProtocolBody)}
	if err = protocol.Decode(payload, req); err != nil {
		return nil, nil, nil, err
	}

	return req, typeReq, resp, nil
}
