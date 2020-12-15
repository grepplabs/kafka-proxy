package protocol

import "fmt"

type SyncGroupRequestFactory struct{}

func (f *SyncGroupRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &SyncGroupRequestV0{}, nil
	case 1:
		return &SyncGroupRequestV1{}, nil
	case 2:
		return &SyncGroupRequestV2{}, nil
	case 3:
		return &SyncGroupRequestV3{}, nil
	case 4:
		return &SyncGroupRequestV4{}, nil
	case 5:
		return &SyncGroupRequestV5{}, nil
	default:
		return nil, fmt.Errorf("Not supported listoffsets request %d", requestKeyVersion.ApiVersion)
	}
}

type SyncGroupRequestV0 struct {
	ConsumerGroups []string
}

func (r *SyncGroupRequestV0) encode(pe packetEncoder) error {
	return nil
}

func (r *SyncGroupRequestV0) key() int16 {
	return 14
}

func (r *SyncGroupRequestV0) version() int16 {
	return 0
}

func (r *SyncGroupRequestV0) decode(pd packetDecoder) (err error) {
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

	// get length of assignments array
	numAssignments, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numAssignments; i++ {
		// member_id
		_, err = pd.getString()
		if err != nil {
			return err
		}

		// assignment
		_, err = pd.getBytes()
		if err != nil {
			return err
		}
	}

	return err
}

func (r *SyncGroupRequestV0) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type SyncGroupRequestV1 struct {
	ConsumerGroups []string
}

func (r *SyncGroupRequestV1) encode(pe packetEncoder) error {
	return nil
}

func (r *SyncGroupRequestV1) key() int16 {
	return 14
}

func (r *SyncGroupRequestV1) version() int16 {
	return 1
}

func (r *SyncGroupRequestV1) decode(pd packetDecoder) (err error) {
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

	// get length of assignments array
	numAssignments, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numAssignments; i++ {
		// member_id
		_, err = pd.getString()
		if err != nil {
			return err
		}

		// assignment
		_, err = pd.getBytes()
		if err != nil {
			return err
		}
	}

	return err
}

func (r *SyncGroupRequestV1) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type SyncGroupRequestV2 struct {
	ConsumerGroups []string
}

func (r *SyncGroupRequestV2) encode(pe packetEncoder) error {
	return nil
}

func (r *SyncGroupRequestV2) key() int16 {
	return 14
}

func (r *SyncGroupRequestV2) version() int16 {
	return 2
}

func (r *SyncGroupRequestV2) decode(pd packetDecoder) (err error) {
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

	// get length of assignments array
	numAssignments, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numAssignments; i++ {
		// member_id
		_, err = pd.getString()
		if err != nil {
			return err
		}

		// assignment
		_, err = pd.getBytes()
		if err != nil {
			return err
		}
	}

	return err
}

func (r *SyncGroupRequestV2) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type SyncGroupRequestV3 struct {
	ConsumerGroups []string
}

func (r *SyncGroupRequestV3) encode(pe packetEncoder) error {
	return nil
}

func (r *SyncGroupRequestV3) key() int16 {
	return 14
}

func (r *SyncGroupRequestV3) version() int16 {
	return 3
}

func (r *SyncGroupRequestV3) decode(pd packetDecoder) (err error) {
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

	// get length of assignments array
	numAssignments, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numAssignments; i++ {
		// member_id
		_, err = pd.getString()
		if err != nil {
			return err
		}

		// assignment
		_, err = pd.getBytes()
		if err != nil {
			return err
		}
	}

	return err
}

func (r *SyncGroupRequestV3) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type SyncGroupRequestV4 struct {
	ConsumerGroups []string
}

func (r *SyncGroupRequestV4) encode(pe packetEncoder) error {
	return nil
}

func (r *SyncGroupRequestV4) key() int16 {
	return 14
}

func (r *SyncGroupRequestV4) version() int16 {
	return 4
}

func (r *SyncGroupRequestV4) decode(pd packetDecoder) (err error) {
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

	// get length of assignments array
	numAssignments, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numAssignments; i++ {
		// member_id
		_, err = pd.getCompactString()
		if err != nil {
			return err
		}

		// assignment
		_, err = pd.getCompactBytes()
		if err != nil {
			return err
		}
	}

	return err
}

func (r *SyncGroupRequestV4) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type SyncGroupRequestV5 struct {
	ConsumerGroups []string
}

func (r *SyncGroupRequestV5) encode(pe packetEncoder) error {
	return nil
}

func (r *SyncGroupRequestV5) key() int16 {
	return 14
}

func (r *SyncGroupRequestV5) version() int16 {
	return 5
}

func (r *SyncGroupRequestV5) decode(pd packetDecoder) (err error) {
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

	// protocol_type
	_, err = pd.getCompactNullableString()
	if err != nil {
		return err
	}

	// protocol_name
	_, err = pd.getCompactNullableString()
	if err != nil {
		return err
	}

	// get length of assignments array
	numAssignments, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numAssignments; i++ {
		// member_id
		_, err = pd.getCompactString()
		if err != nil {
			return err
		}

		// assignment
		_, err = pd.getCompactBytes()
		if err != nil {
			return err
		}
	}

	return err
}

func (r *SyncGroupRequestV5) GetConsumerGroups() []string {
	return r.ConsumerGroups
}
