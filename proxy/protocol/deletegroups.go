package protocol

import "fmt"

type DeleteGroupsRequestFactory struct{}

func (f *DeleteGroupsRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &DeleteGroupsRequestV0{}, nil
	case 1:
		return &DeleteGroupsRequestV1{}, nil
	case 2:
		return &DeleteGroupsRequestV2{}, nil
	default:
		return nil, fmt.Errorf("Not supported listoffsets request %d", requestKeyVersion.ApiVersion)
	}
}

type DeleteGroupsRequestV0 struct {
	ConsumerGroups []string
}

func (r *DeleteGroupsRequestV0) encode(pe packetEncoder) error {
	return nil
}

func (r *DeleteGroupsRequestV0) key() int16 {
	return 42
}

func (r *DeleteGroupsRequestV0) version() int16 {
	return 0
}

func (r *DeleteGroupsRequestV0) decode(pd packetDecoder) (err error) {
	// groups
	groups, err := pd.getStringArray()
	if err != nil {
		return err
	}

	r.ConsumerGroups = groups

	return err
}

func (r *DeleteGroupsRequestV0) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type DeleteGroupsRequestV1 struct {
	ConsumerGroups []string
}

func (r *DeleteGroupsRequestV1) encode(pe packetEncoder) error {
	return nil
}

func (r *DeleteGroupsRequestV1) key() int16 {
	return 42
}

func (r *DeleteGroupsRequestV1) version() int16 {
	return 1
}

func (r *DeleteGroupsRequestV1) decode(pd packetDecoder) (err error) {
	// groups
	groups, err := pd.getStringArray()
	if err != nil {
		return err
	}

	r.ConsumerGroups = groups

	return err
}

func (r *DeleteGroupsRequestV1) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type DeleteGroupsRequestV2 struct {
	ConsumerGroups []string
}

func (r *DeleteGroupsRequestV2) encode(pe packetEncoder) error {
	return nil
}

func (r *DeleteGroupsRequestV2) key() int16 {
	return 42
}

func (r *DeleteGroupsRequestV2) version() int16 {
	return 2
}

func (r *DeleteGroupsRequestV2) decode(pd packetDecoder) (err error) {
	// groups
	groups, err := pd.getCompactStringArray()
	if err != nil {
		return err
	}

	r.ConsumerGroups = groups

	return err
}

func (r *DeleteGroupsRequestV2) GetConsumerGroups() []string {
	return r.ConsumerGroups
}
