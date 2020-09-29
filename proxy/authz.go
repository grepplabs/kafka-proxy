package proxy

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"github.com/sirupsen/logrus"
)

type Authz struct {
	authzProvider apis.AuthzProvider
	enabled       bool
}

func getPartialDecodedRequest(keyVersionBuf []byte, src io.Reader) (clientID string, reqBody protocol.ProtocolBody, pay []byte, err error) {
	if len(keyVersionBuf) != 8 {
		return "", nil, nil, errors.New("length of keyVersionBuf should be 8")
	}
	// keyVersionBuf has already been read from connection
	requestKeyVersion := &protocol.RequestKeyVersion{}
	if err = protocol.Decode(keyVersionBuf, requestKeyVersion); err != nil {
		return "", nil, nil, err
	}

	if int32(requestKeyVersion.Length) > protocol.MaxRequestSize {
		return "", nil, nil, protocol.PacketDecodingError{Info: fmt.Sprintf("message of length %d too large", requestKeyVersion.Length)}
	}

	resp := make([]byte, int(requestKeyVersion.Length-4))
	if _, err = io.ReadFull(src, resp); err != nil {
		return "", nil, nil, err
	}

	payload := bytes.Join([][]byte{keyVersionBuf[4:], resp}, nil)

	factory := &protocol.RequestTypeFactory{}
	typeReq, err := factory.Produce(requestKeyVersion)

	logrus.Debugf("Factory is %T", typeReq)

	if err != nil {
		return "", nil, nil, err
	}

	headerVer := requestKeyVersion.RequestHeaderVersion()

	switch headerVer {
	case 0:
		req := protocol.Request{Body: typeReq.(protocol.ProtocolBody)}
		if err = protocol.Decode(payload, &req); err != nil {
			return "", nil, nil, err
		}
		return req.ClientID, typeReq, resp, nil
	case 1:
		req := protocol.Request{Body: typeReq.(protocol.ProtocolBody)}
		if err = protocol.Decode(payload, &req); err != nil {
			return "", nil, nil, err
		}
		return req.ClientID, typeReq, resp, nil
	case 2:
		req := protocol.RequestV2{Body: typeReq.(protocol.ProtocolBody)}
		if err = protocol.Decode(payload, &req); err != nil {
			return "", nil, nil, err
		}
		return req.ClientID, typeReq, resp, nil
	default:
		err = fmt.Errorf("Unsupported header version %d", headerVer)
		return "", nil, nil, err
	}
}
