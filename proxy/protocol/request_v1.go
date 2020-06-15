package protocol

import "fmt"

type ProtocolBody interface {
	encoder
	decoder
	key() int16
	version() int16
}

type Request struct {
	CorrelationID int32
	ClientID      string
	Body          ProtocolBody
}

func (r *Request) encode(pe packetEncoder) (err error) {
	pe.putInt16(r.Body.key())
	pe.putInt16(r.Body.version())
	pe.putInt32(r.CorrelationID)
	err = pe.putString(r.ClientID)
	if err != nil {
		return err
	}
	err = r.Body.encode(pe)
	if err != nil {
		return err
	}
	return err
}

func (r *Request) decode(pd packetDecoder) (err error) {
	if r.Body == nil {
		return PacketDecodingError{"unknown body decoder"}
	}
	var key int16
	if key, err = pd.getInt16(); err != nil {
		return err
	}
	var version int16
	if version, err = pd.getInt16(); err != nil {
		return err
	}
	if r.Body.key() != key || r.Body.version() != version {
		return PacketDecodingError{fmt.Sprintf("expected request key,version %d,%d but got %d,%d", r.Body.key(), r.Body.version(), key, version)}
	}
	if r.CorrelationID, err = pd.getInt32(); err != nil {
		return err
	}
	if r.ClientID, err = pd.getString(); err != nil {
		return err
	}
	return r.Body.decode(pd)
}

type TopicRequestInterface interface {
	GetTopics() []string
}

type RequestTypeFactory struct{}

func (*RequestTypeFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiKey {
	case 0:
		factory := &ProduceRequestFactory{}
		req, err := factory.Produce(requestKeyVersion)

		if err != nil {
			return nil, err
		}

		return req, nil
	case 1:
		factory := &FetchRequestFactory{}
		req, err := factory.Produce(requestKeyVersion)

		if err != nil {
			return nil, err
		}

		return req, nil
	case 2:
		factory := &ListOffsetsRequestFactory{}
		req, err := factory.Produce(requestKeyVersion)

		if err != nil {
			return nil, err
		}

		return req, nil
	case 3:
		factory := &TopicMetadataRequestFactory{}
		req, err := factory.Produce(requestKeyVersion)

		if err != nil {
			return nil, err
		}

		return req, nil
	case 8:
		factory := &OffsetCommitRequestFactory{}
		req, err := factory.Produce(requestKeyVersion)

		if err != nil {
			return nil, err
		}

		return req, nil
	case 9:
		factory := &OffsetFetchRequestFactory{}
		req, err := factory.Produce(requestKeyVersion)

		if err != nil {
			return nil, err
		}

		return req, nil
	case 19:
		factory := &CreateTopicsRequestFactory{}
		req, err := factory.Produce(requestKeyVersion)

		if err != nil {
			return nil, err
		}

		return req, nil
	case 20:
		factory := &DeleteTopicsRequestFactory{}
		req, err := factory.Produce(requestKeyVersion)

		if err != nil {
			return nil, err
		}

		return req, nil
	case 37:
		factory := &CreatePartitionsRequestFactory{}
		req, err := factory.Produce(requestKeyVersion)

		if err != nil {
			return nil, err
		}

		return req, nil
	default:
		return nil, fmt.Errorf("Unsupported apikey for producing RequestType %d", requestKeyVersion.ApiKey)
	}
}
