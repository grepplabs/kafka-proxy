package protocol

import "fmt"

type LeaveGroupRequestFactory struct{}

func (f *LeaveGroupRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &LeaveGroupRequestV0{}, nil
	case 1:
		return &LeaveGroupRequestV1{}, nil
	case 2:
		return &LeaveGroupRequestV2{}, nil
	case 3:
		return &LeaveGroupRequestV3{}, nil
	case 4:
		return &LeaveGroupRequestV4{}, nil
	default:
		return nil, fmt.Errorf("Not supported listoffsets request %d", requestKeyVersion.ApiVersion)
	}
}

type LeaveGroupRequestV0 struct {
	ConsumerGroups []string
}

func (r *LeaveGroupRequestV0) encode(pe packetEncoder) error {
	return nil
}

func (r *LeaveGroupRequestV0) key() int16 {
	return 13
}

func (r *LeaveGroupRequestV0) version() int16 {
	return 0
}

func (r *LeaveGroupRequestV0) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)

	// member_id
	_, err = pd.getString()
	if err != nil {
		return err
	}

	return err
}

func (r *LeaveGroupRequestV0) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type LeaveGroupRequestV1 struct {
	ConsumerGroups []string
}

func (r *LeaveGroupRequestV1) encode(pe packetEncoder) error {
	return nil
}

func (r *LeaveGroupRequestV1) key() int16 {
	return 13
}

func (r *LeaveGroupRequestV1) version() int16 {
	return 1
}

func (r *LeaveGroupRequestV1) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)

	// member_id
	_, err = pd.getString()
	if err != nil {
		return err
	}

	return err
}

func (r *LeaveGroupRequestV1) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type LeaveGroupRequestV2 struct {
	ConsumerGroups []string
}

func (r *LeaveGroupRequestV2) encode(pe packetEncoder) error {
	return nil
}

func (r *LeaveGroupRequestV2) key() int16 {
	return 13
}

func (r *LeaveGroupRequestV2) version() int16 {
	return 2
}

func (r *LeaveGroupRequestV2) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)

	// member_id
	_, err = pd.getString()
	if err != nil {
		return err
	}

	return err
}

func (r *LeaveGroupRequestV2) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type LeaveGroupRequestV3 struct {
	ConsumerGroups []string
}

func (r *LeaveGroupRequestV3) encode(pe packetEncoder) error {
	return nil
}

func (r *LeaveGroupRequestV3) key() int16 {
	return 13
}

func (r *LeaveGroupRequestV3) version() int16 {
	return 3
}

func (r *LeaveGroupRequestV3) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)

	// get length of members array
	numMembers, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numMembers; i++ {
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
	}

	return err
}

func (r *LeaveGroupRequestV3) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type LeaveGroupRequestV4 struct {
	ConsumerGroups []string
}

func (r *LeaveGroupRequestV4) encode(pe packetEncoder) error {
	return nil
}

func (r *LeaveGroupRequestV4) key() int16 {
	return 13
}

func (r *LeaveGroupRequestV4) version() int16 {
	return 4
}

func (r *LeaveGroupRequestV4) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getCompactString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)

	// get length of members array
	numMembers, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numMembers; i++ {
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
	}

	return err
}

func (r *LeaveGroupRequestV4) GetConsumerGroups() []string {
	return r.ConsumerGroups
}
