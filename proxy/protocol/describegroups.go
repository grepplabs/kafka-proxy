package protocol

import "fmt"

type DescribeGroupsRequestFactory struct{}

func (f *DescribeGroupsRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &DescribeGroupsRequestV0{}, nil
	case 1:
		return &DescribeGroupsRequestV1{}, nil
	case 2:
		return &DescribeGroupsRequestV2{}, nil
	case 3:
		return &DescribeGroupsRequestV3{}, nil
	case 4:
		return &DescribeGroupsRequestV4{}, nil
	default:
		return nil, fmt.Errorf("Not supported listoffsets request %d", requestKeyVersion.ApiVersion)
	}
}

type DescribeGroupsRequestV0 struct {
	ConsumerGroups []string
}

func (r *DescribeGroupsRequestV0) encode(pe packetEncoder) error {
	return nil
}

func (r *DescribeGroupsRequestV0) key() int16 {
	return 15
}

func (r *DescribeGroupsRequestV0) version() int16 {
	return 0
}

func (r *DescribeGroupsRequestV0) decode(pd packetDecoder) (err error) {
	// groups
	groups, err := pd.getStringArray()
	if err != nil {
		return err
	}

	r.ConsumerGroups = groups

	return err
}

func (r *DescribeGroupsRequestV0) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type DescribeGroupsRequestV1 struct {
	ConsumerGroups []string
}

func (r *DescribeGroupsRequestV1) encode(pe packetEncoder) error {
	return nil
}

func (r *DescribeGroupsRequestV1) key() int16 {
	return 15
}

func (r *DescribeGroupsRequestV1) version() int16 {
	return 1
}

func (r *DescribeGroupsRequestV1) decode(pd packetDecoder) (err error) {
	// groups
	groups, err := pd.getStringArray()
	if err != nil {
		return err
	}

	r.ConsumerGroups = groups

	return err
}

func (r *DescribeGroupsRequestV1) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type DescribeGroupsRequestV2 struct {
	ConsumerGroups []string
}

func (r *DescribeGroupsRequestV2) encode(pe packetEncoder) error {
	return nil
}

func (r *DescribeGroupsRequestV2) key() int16 {
	return 15
}

func (r *DescribeGroupsRequestV2) version() int16 {
	return 2
}

func (r *DescribeGroupsRequestV2) decode(pd packetDecoder) (err error) {
	// groups
	groups, err := pd.getStringArray()
	if err != nil {
		return err
	}

	r.ConsumerGroups = groups

	return err
}

func (r *DescribeGroupsRequestV2) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type DescribeGroupsRequestV3 struct {
	ConsumerGroups []string
}

func (r *DescribeGroupsRequestV3) encode(pe packetEncoder) error {
	return nil
}

func (r *DescribeGroupsRequestV3) key() int16 {
	return 15
}

func (r *DescribeGroupsRequestV3) version() int16 {
	return 3
}

func (r *DescribeGroupsRequestV3) decode(pd packetDecoder) (err error) {
	// groups
	groups, err := pd.getStringArray()
	if err != nil {
		return err
	}

	r.ConsumerGroups = groups

	// include_authorized_operations
	_, err = pd.getBool()
	if err != nil {
		return err
	}

	return err
}

func (r *DescribeGroupsRequestV3) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type DescribeGroupsRequestV4 struct {
	ConsumerGroups []string
}

func (r *DescribeGroupsRequestV4) encode(pe packetEncoder) error {
	return nil
}

func (r *DescribeGroupsRequestV4) key() int16 {
	return 15
}

func (r *DescribeGroupsRequestV4) version() int16 {
	return 4
}

func (r *DescribeGroupsRequestV4) decode(pd packetDecoder) (err error) {
	// groups
	groups, err := pd.getStringArray()
	if err != nil {
		return err
	}

	r.ConsumerGroups = groups

	// include_authorized_operations
	_, err = pd.getBool()
	if err != nil {
		return err
	}

	return err
}

func (r *DescribeGroupsRequestV4) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type DescribeGroupsRequestV5 struct {
	ConsumerGroups []string
}

func (r *DescribeGroupsRequestV5) encode(pe packetEncoder) error {
	return nil
}

func (r *DescribeGroupsRequestV5) key() int16 {
	return 15
}

func (r *DescribeGroupsRequestV5) version() int16 {
	return 5
}

func (r *DescribeGroupsRequestV5) decode(pd packetDecoder) (err error) {
	// groups
	groups, err := pd.getCompactStringArray()
	if err != nil {
		return err
	}

	r.ConsumerGroups = groups

	// include_authorized_operations
	_, err = pd.getBool()
	if err != nil {
		return err
	}

	return err
}

func (r *DescribeGroupsRequestV5) GetConsumerGroups() []string {
	return r.ConsumerGroups
}
