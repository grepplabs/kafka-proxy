package protocol

import "fmt"

type JoinGroupRequestFactory struct{}

func (f *JoinGroupRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &JoinGroupRequestV0{}, nil
	case 1:
		return &JoinGroupRequestV1{}, nil
	case 2:
		return &JoinGroupRequestV2{}, nil
	case 3:
		return &JoinGroupRequestV3{}, nil
	case 4:
		return &JoinGroupRequestV4{}, nil
	case 5:
		return &JoinGroupRequestV5{}, nil
	case 6:
		return &JoinGroupRequestV6{}, nil
	case 7:
		return &JoinGroupRequestV7{}, nil
	default:
		return nil, fmt.Errorf("Not supported listoffsets request %d", requestKeyVersion.ApiVersion)
	}
}

type JoinGroupRequestV0 struct {
	ConsumerGroups []string
}

func (r *JoinGroupRequestV0) encode(pe packetEncoder) error {
	return nil
}

func (r *JoinGroupRequestV0) key() int16 {
	return 11
}

func (r *JoinGroupRequestV0) version() int16 {
	return 0
}

func (r *JoinGroupRequestV0) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)
	// get length of topic array
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// member_id
	_, err = pd.getString()
	if err != nil {
		return err
	}

	// protocolType
	_, err = pd.getString()
	if err != nil {
		return err
	}

	// get length of protocols array
	numProtocols, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numProtocols; i++ {
		// protocol name
		_, err = pd.getString()
		if err != nil {
			return err
		}

		// metadata
		_, err = pd.getBytes()
		if err != nil {
			return err
		}
	}

	return err
}

func (r *JoinGroupRequestV0) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type JoinGroupRequestV1 struct {
	ConsumerGroups []string
}

func (r *JoinGroupRequestV1) encode(pe packetEncoder) error {
	return nil
}

func (r *JoinGroupRequestV1) key() int16 {
	return 11
}

func (r *JoinGroupRequestV1) version() int16 {
	return 1
}

func (r *JoinGroupRequestV1) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)
	// session_timeout_ms
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// rebalance_timeout_ms
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// member_id
	_, err = pd.getString()
	if err != nil {
		return err
	}

	// protocolType
	_, err = pd.getString()
	if err != nil {
		return err
	}

	// get length of protocols array
	numProtocols, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numProtocols; i++ {
		// protocol name
		_, err = pd.getString()
		if err != nil {
			return err
		}

		// metadata
		_, err = pd.getBytes()
		if err != nil {
			return err
		}
	}

	return err
}

func (r *JoinGroupRequestV1) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type JoinGroupRequestV2 struct {
	ConsumerGroups []string
}

func (r *JoinGroupRequestV2) encode(pe packetEncoder) error {
	return nil
}

func (r *JoinGroupRequestV2) key() int16 {
	return 11
}

func (r *JoinGroupRequestV2) version() int16 {
	return 2
}

func (r *JoinGroupRequestV2) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)
	// session_timeout_ms
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// rebalance_timeout_ms
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// member_id
	_, err = pd.getString()
	if err != nil {
		return err
	}

	// protocolType
	_, err = pd.getString()
	if err != nil {
		return err
	}

	// get length of protocols array
	numProtocols, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numProtocols; i++ {
		// protocol name
		_, err = pd.getString()
		if err != nil {
			return err
		}

		// metadata
		_, err = pd.getBytes()
		if err != nil {
			return err
		}
	}

	return err
}

func (r *JoinGroupRequestV2) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type JoinGroupRequestV3 struct {
	ConsumerGroups []string
}

func (r *JoinGroupRequestV3) encode(pe packetEncoder) error {
	return nil
}

func (r *JoinGroupRequestV3) key() int16 {
	return 11
}

func (r *JoinGroupRequestV3) version() int16 {
	return 3
}

func (r *JoinGroupRequestV3) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)
	// session_timeout_ms
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// rebalance_timeout_ms
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// member_id
	_, err = pd.getString()
	if err != nil {
		return err
	}

	// protocolType
	_, err = pd.getString()
	if err != nil {
		return err
	}

	// get length of protocols array
	numProtocols, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numProtocols; i++ {
		// protocol name
		_, err = pd.getString()
		if err != nil {
			return err
		}

		// metadata
		_, err = pd.getBytes()
		if err != nil {
			return err
		}
	}

	return err
}

func (r *JoinGroupRequestV3) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type JoinGroupRequestV4 struct {
	ConsumerGroups []string
}

func (r *JoinGroupRequestV4) encode(pe packetEncoder) error {
	return nil
}

func (r *JoinGroupRequestV4) key() int16 {
	return 11
}

func (r *JoinGroupRequestV4) version() int16 {
	return 4
}

func (r *JoinGroupRequestV4) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)
	// session_timeout_ms
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// rebalance_timeout_ms
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// member_id
	_, err = pd.getString()
	if err != nil {
		return err
	}

	// protocolType
	_, err = pd.getString()
	if err != nil {
		return err
	}

	// get length of protocols array
	numProtocols, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numProtocols; i++ {
		// protocol name
		_, err = pd.getString()
		if err != nil {
			return err
		}

		// metadata
		_, err = pd.getBytes()
		if err != nil {
			return err
		}
	}

	return err
}

func (r *JoinGroupRequestV4) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type JoinGroupRequestV5 struct {
	ConsumerGroups []string
}

func (r *JoinGroupRequestV5) encode(pe packetEncoder) error {
	return nil
}

func (r *JoinGroupRequestV5) key() int16 {
	return 11
}

func (r *JoinGroupRequestV5) version() int16 {
	return 5
}

func (r *JoinGroupRequestV5) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)
	// session_timeout_ms
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// rebalance_timeout_ms
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

	// protocolType
	_, err = pd.getString()
	if err != nil {
		return err
	}

	// get length of protocols array
	numProtocols, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numProtocols; i++ {
		// protocol name
		_, err = pd.getString()
		if err != nil {
			return err
		}

		// metadata
		_, err = pd.getBytes()
		if err != nil {
			return err
		}
	}

	return err
}

func (r *JoinGroupRequestV5) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type JoinGroupRequestV6 struct {
	ConsumerGroups []string
}

func (r *JoinGroupRequestV6) encode(pe packetEncoder) error {
	return nil
}

func (r *JoinGroupRequestV6) key() int16 {
	return 11
}

func (r *JoinGroupRequestV6) version() int16 {
	return 6
}

func (r *JoinGroupRequestV6) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getCompactString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)
	// session_timeout_ms
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// rebalance_timeout_ms
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

	// protocolType
	_, err = pd.getCompactString()
	if err != nil {
		return err
	}

	// get length of protocols array
	numProtocols, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numProtocols; i++ {
		// protocol name
		_, err = pd.getCompactString()
		if err != nil {
			return err
		}

		// metadata
		_, err = pd.getCompactBytes()
		if err != nil {
			return err
		}
	}

	return err
}

func (r *JoinGroupRequestV6) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type JoinGroupRequestV7 struct {
	ConsumerGroups []string
}

func (r *JoinGroupRequestV7) encode(pe packetEncoder) error {
	return nil
}

func (r *JoinGroupRequestV7) key() int16 {
	return 11
}

func (r *JoinGroupRequestV7) version() int16 {
	return 7
}

func (r *JoinGroupRequestV7) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getCompactString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)
	// session_timeout_ms
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// rebalance_timeout_ms
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

	// protocolType
	_, err = pd.getCompactString()
	if err != nil {
		return err
	}

	// get length of protocols array
	numProtocols, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numProtocols; i++ {
		// protocol name
		_, err = pd.getCompactString()
		if err != nil {
			return err
		}

		// metadata
		_, err = pd.getCompactBytes()
		if err != nil {
			return err
		}
	}

	return err
}

func (r *JoinGroupRequestV7) GetConsumerGroups() []string {
	return r.ConsumerGroups
}
