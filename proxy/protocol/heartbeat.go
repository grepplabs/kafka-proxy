package protocol

import "fmt"

type HeartbeatRequestFactory struct{}

func (f *HeartbeatRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &HeartbeatRequestV0{}, nil
	case 1:
		return &HeartbeatRequestV1{}, nil
	case 2:
		return &HeartbeatRequestV2{}, nil
	case 3:
		return &HeartbeatRequestV3{}, nil
	case 4:
		return &HeartbeatRequestV4{}, nil
	default:
		return nil, fmt.Errorf("Not supported listoffsets request %d", requestKeyVersion.ApiVersion)
	}
}

type HeartbeatRequestV0 struct {
	ConsumerGroups []string
}

func (r *HeartbeatRequestV0) encode(pe packetEncoder) error {
	return nil
}

func (r *HeartbeatRequestV0) key() int16 {
	return 12
}

func (r *HeartbeatRequestV0) version() int16 {
	return 0
}

func (r *HeartbeatRequestV0) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)
	// generation_id
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// member_id
	_, err = pd.getString()
	if err != nil {
		return err
	}

	return err
}

func (r *HeartbeatRequestV0) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type HeartbeatRequestV1 struct {
	ConsumerGroups []string
}

func (r *HeartbeatRequestV1) encode(pe packetEncoder) error {
	return nil
}

func (r *HeartbeatRequestV1) key() int16 {
	return 12
}

func (r *HeartbeatRequestV1) version() int16 {
	return 1
}

func (r *HeartbeatRequestV1) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)
	// generation_id
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// member_id
	_, err = pd.getString()
	if err != nil {
		return err
	}

	return err
}

func (r *HeartbeatRequestV1) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type HeartbeatRequestV2 struct {
	ConsumerGroups []string
}

func (r *HeartbeatRequestV2) encode(pe packetEncoder) error {
	return nil
}

func (r *HeartbeatRequestV2) key() int16 {
	return 12
}

func (r *HeartbeatRequestV2) version() int16 {
	return 2
}

func (r *HeartbeatRequestV2) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)
	// generation_id
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// member_id
	_, err = pd.getString()
	if err != nil {
		return err
	}

	return err
}

func (r *HeartbeatRequestV2) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type HeartbeatRequestV3 struct {
	ConsumerGroups []string
}

func (r *HeartbeatRequestV3) encode(pe packetEncoder) error {
	return nil
}

func (r *HeartbeatRequestV3) key() int16 {
	return 12
}

func (r *HeartbeatRequestV3) version() int16 {
	return 3
}

func (r *HeartbeatRequestV3) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)
	// generation_id
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// member_id
	_, err = pd.getString()
	if err != nil {
		return err
	}

	// group_instance_id
	_, err = pd.getNullableString()
	if err != nil {
		return err
	}

	return err
}

func (r *HeartbeatRequestV3) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type HeartbeatRequestV4 struct {
	ConsumerGroups []string
}

func (r *HeartbeatRequestV4) encode(pe packetEncoder) error {
	return nil
}

func (r *HeartbeatRequestV4) key() int16 {
	return 12
}

func (r *HeartbeatRequestV4) version() int16 {
	return 4
}

func (r *HeartbeatRequestV4) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getCompactString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)
	// generation_id
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// member_id
	_, err = pd.getCompactString()
	if err != nil {
		return err
	}

	// group_instance_id
	_, err = pd.getCompactNullableString()
	if err != nil {
		return err
	}

	return err
}

func (r *HeartbeatRequestV4) GetConsumerGroups() []string {
	return r.ConsumerGroups
}
