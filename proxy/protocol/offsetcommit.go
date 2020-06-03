package protocol

import "fmt"

type OffsetCommitRequestFactory struct{}

func (f *OffsetCommitRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &OffsetCommitRequestV0{}, nil
	case 1:
		return &OffsetCommitRequestV1{}, nil
	case 2:
		return &OffsetCommitRequestV2{}, nil
	case 3:
		return &OffsetCommitRequestV3{}, nil
	case 4:
		return &OffsetCommitRequestV4{}, nil
	case 5:
		return &OffsetCommitRequestV5{}, nil
	case 6:
		return &OffsetCommitRequestV6{}, nil
	case 7:
		return &OffsetCommitRequestV7{}, nil
	default:
		return nil, fmt.Errorf("Not supported listoffsets request %d", requestKeyVersion.ApiVersion)
	}
}

type OffsetCommitRequestV0 struct {
	Topics []string
}

func (r *OffsetCommitRequestV0) encode(pe packetEncoder) error {
	return nil
}

func (r *OffsetCommitRequestV0) key() int16 {
	return 8
}

func (r *OffsetCommitRequestV0) version() int16 {
	return 0
}

func (r *OffsetCommitRequestV0) decode(pd packetDecoder) (err error) {
	// group_id
	_, err = pd.getString()
	if err != nil {
		return err
	}

	// get length of topic array
	numTopics, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numTopics; i++ {
		topicName, err := pd.getString()
		if err != nil {
			return err
		}

		r.Topics = append(r.Topics, topicName)

		// get length of partition array
		numPart, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numPart; j++ {
			// partition_index
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// committed_offset
			_, err := pd.getInt64()
			if err != nil {
				return err
			}
			// committed_metadata
			_, err = pd.getNullableString()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *OffsetCommitRequestV0) GetTopics() []string {
	return r.Topics
}

type OffsetCommitRequestV1 struct {
	Topics []string
}

func (r *OffsetCommitRequestV1) encode(pe packetEncoder) error {
	return nil
}

func (r *OffsetCommitRequestV1) key() int16 {
	return 8
}

func (r *OffsetCommitRequestV1) version() int16 {
	return 1
}

func (r *OffsetCommitRequestV1) decode(pd packetDecoder) (err error) {
	// group_id
	_, err = pd.getString()
	if err != nil {
		return err
	}
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

	// get length of topic array
	numTopics, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numTopics; i++ {
		topicName, err := pd.getString()
		if err != nil {
			return err
		}

		r.Topics = append(r.Topics, topicName)

		// get length of partition array
		numPart, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numPart; j++ {
			// partition_index
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// committed_offset
			_, err := pd.getInt64()
			if err != nil {
				return err
			}
			// commit_timestamp
			_, err = pd.getInt64()
			if err != nil {
				return err
			}
			// committed_metadata
			_, err = pd.getNullableString()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *OffsetCommitRequestV1) GetTopics() []string {
	return r.Topics
}

type OffsetCommitRequestV2 struct {
	Topics []string
}

func (r *OffsetCommitRequestV2) encode(pe packetEncoder) error {
	return nil
}

func (r *OffsetCommitRequestV2) key() int16 {
	return 8
}

func (r *OffsetCommitRequestV2) version() int16 {
	return 2
}

func (r *OffsetCommitRequestV2) decode(pd packetDecoder) (err error) {
	// group_id
	_, err = pd.getString()
	if err != nil {
		return err
	}
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
	// retention_time_ms
	_, err = pd.getInt64()
	if err != nil {
		return err
	}

	// get length of topic array
	numTopics, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numTopics; i++ {
		topicName, err := pd.getString()
		if err != nil {
			return err
		}

		r.Topics = append(r.Topics, topicName)

		// get length of partition array
		numPart, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numPart; j++ {
			// partition_index
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// committed_offset
			_, err := pd.getInt64()
			if err != nil {
				return err
			}
			// committed_metadata
			_, err = pd.getNullableString()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *OffsetCommitRequestV2) GetTopics() []string {
	return r.Topics
}

type OffsetCommitRequestV3 struct {
	Topics []string
}

func (r *OffsetCommitRequestV3) encode(pe packetEncoder) error {
	return nil
}

func (r *OffsetCommitRequestV3) key() int16 {
	return 8
}

func (r *OffsetCommitRequestV3) version() int16 {
	return 3
}

func (r *OffsetCommitRequestV3) decode(pd packetDecoder) (err error) {
	// group_id
	_, err = pd.getString()
	if err != nil {
		return err
	}
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
	// retention_time_ms
	_, err = pd.getInt64()
	if err != nil {
		return err
	}

	// get length of topic array
	numTopics, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numTopics; i++ {
		topicName, err := pd.getString()
		if err != nil {
			return err
		}

		r.Topics = append(r.Topics, topicName)

		// get length of partition array
		numPart, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numPart; j++ {
			// partition_index
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// committed_offset
			_, err := pd.getInt64()
			if err != nil {
				return err
			}
			// committed_metadata
			_, err = pd.getNullableString()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *OffsetCommitRequestV3) GetTopics() []string {
	return r.Topics
}

type OffsetCommitRequestV4 struct {
	Topics []string
}

func (r *OffsetCommitRequestV4) encode(pe packetEncoder) error {
	return nil
}

func (r *OffsetCommitRequestV4) key() int16 {
	return 8
}

func (r *OffsetCommitRequestV4) version() int16 {
	return 4
}

func (r *OffsetCommitRequestV4) decode(pd packetDecoder) (err error) {
	// group_id
	_, err = pd.getString()
	if err != nil {
		return err
	}
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
	// retention_time_ms
	_, err = pd.getInt64()
	if err != nil {
		return err
	}

	// get length of topic array
	numTopics, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numTopics; i++ {
		topicName, err := pd.getString()
		if err != nil {
			return err
		}

		r.Topics = append(r.Topics, topicName)

		// get length of partition array
		numPart, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numPart; j++ {
			// partition_index
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// committed_offset
			_, err := pd.getInt64()
			if err != nil {
				return err
			}
			// committed_metadata
			_, err = pd.getNullableString()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *OffsetCommitRequestV4) GetTopics() []string {
	return r.Topics
}

type OffsetCommitRequestV5 struct {
	Topics []string
}

func (r *OffsetCommitRequestV5) encode(pe packetEncoder) error {
	return nil
}

func (r *OffsetCommitRequestV5) key() int16 {
	return 8
}

func (r *OffsetCommitRequestV5) version() int16 {
	return 5
}

func (r *OffsetCommitRequestV5) decode(pd packetDecoder) (err error) {
	// group_id
	_, err = pd.getString()
	if err != nil {
		return err
	}
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

	// get length of topic array
	numTopics, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numTopics; i++ {
		topicName, err := pd.getString()
		if err != nil {
			return err
		}

		r.Topics = append(r.Topics, topicName)

		// get length of partition array
		numPart, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numPart; j++ {
			// partition_index
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// committed_offset
			_, err := pd.getInt64()
			if err != nil {
				return err
			}
			// committed_metadata
			_, err = pd.getNullableString()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *OffsetCommitRequestV5) GetTopics() []string {
	return r.Topics
}

type OffsetCommitRequestV6 struct {
	Topics []string
}

func (r *OffsetCommitRequestV6) encode(pe packetEncoder) error {
	return nil
}

func (r *OffsetCommitRequestV6) key() int16 {
	return 8
}

func (r *OffsetCommitRequestV6) version() int16 {
	return 6
}

func (r *OffsetCommitRequestV6) decode(pd packetDecoder) (err error) {
	// group_id
	_, err = pd.getString()
	if err != nil {
		return err
	}
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

	// get length of topic array
	numTopics, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numTopics; i++ {
		topicName, err := pd.getString()
		if err != nil {
			return err
		}

		r.Topics = append(r.Topics, topicName)

		// get length of partition array
		numPart, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numPart; j++ {
			// partition_index
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// committed_offset
			_, err := pd.getInt64()
			if err != nil {
				return err
			}
			// committed_leader_epoch
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// committed_metadata
			_, err = pd.getNullableString()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *OffsetCommitRequestV6) GetTopics() []string {
	return r.Topics
}

type OffsetCommitRequestV7 struct {
	Topics []string
}

func (r *OffsetCommitRequestV7) encode(pe packetEncoder) error {
	return nil
}

func (r *OffsetCommitRequestV7) key() int16 {
	return 8
}

func (r *OffsetCommitRequestV7) version() int16 {
	return 7
}

func (r *OffsetCommitRequestV7) decode(pd packetDecoder) (err error) {
	// group_id
	_, err = pd.getString()
	if err != nil {
		return err
	}
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

	// get length of topic array
	numTopics, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numTopics; i++ {
		topicName, err := pd.getString()
		if err != nil {
			return err
		}

		r.Topics = append(r.Topics, topicName)

		// get length of partition array
		numPart, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numPart; j++ {
			// partition_index
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// committed_offset
			_, err := pd.getInt64()
			if err != nil {
				return err
			}
			// committed_leader_epoch
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// committed_metadata
			_, err = pd.getNullableString()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *OffsetCommitRequestV7) GetTopics() []string {
	return r.Topics
}
