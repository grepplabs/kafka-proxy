package protocol

import "fmt"

type TxnOffsetCommitRequestFactory struct{}

func (f *TxnOffsetCommitRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &TxnOffsetCommitV0{}, nil
	case 1:
		return &TxnOffsetCommitV1{}, nil
	default:
		return nil, fmt.Errorf("Not supported fetch request %d", requestKeyVersion.ApiVersion)
	}
}

type TxnOffsetCommitV0 struct {
	Topics         []string
	ConsumerGroups []string
}

func (r *TxnOffsetCommitV0) encode(pe packetEncoder) error {
	return nil
}

func (r *TxnOffsetCommitV0) key() int16 {
	return 28
}

func (r *TxnOffsetCommitV0) version() int16 {
	return 0
}

func (r *TxnOffsetCommitV0) decode(pd packetDecoder) (err error) {
	// transactional_id
	_, err = pd.getString()
	if err != nil {
		return err
	}
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)
	// producer_id
	_, err = pd.getInt64()
	if err != nil {
		return err
	}
	// producer_epoch
	_, err = pd.getInt16()
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

func (r *TxnOffsetCommitV0) GetTopics() []string {
	return r.Topics
}

func (r *TxnOffsetCommitV0) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type TxnOffsetCommitV1 struct {
	Topics         []string
	ConsumerGroups []string
}

func (r *TxnOffsetCommitV1) encode(pe packetEncoder) error {
	return nil
}

func (r *TxnOffsetCommitV1) key() int16 {
	return 28
}

func (r *TxnOffsetCommitV1) version() int16 {
	return 1
}

func (r *TxnOffsetCommitV1) decode(pd packetDecoder) (err error) {
	// transactional_id
	_, err = pd.getString()
	if err != nil {
		return err
	}
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)

	// producer_id
	_, err = pd.getInt64()
	if err != nil {
		return err
	}
	// producer_epoch
	_, err = pd.getInt16()
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

func (r *TxnOffsetCommitV1) GetTopics() []string {
	return r.Topics
}

func (r *TxnOffsetCommitV1) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type TxnOffsetCommitV2 struct {
	Topics         []string
	ConsumerGroups []string
}

func (r *TxnOffsetCommitV2) encode(pe packetEncoder) error {
	return nil
}

func (r *TxnOffsetCommitV2) key() int16 {
	return 28
}

func (r *TxnOffsetCommitV2) version() int16 {
	return 2
}

func (r *TxnOffsetCommitV2) decode(pd packetDecoder) (err error) {
	// transactional_id
	_, err = pd.getString()
	if err != nil {
		return err
	}
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)

	// producer_id
	_, err = pd.getInt64()
	if err != nil {
		return err
	}
	// producer_epoch
	_, err = pd.getInt16()
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
			_, err = pd.getInt64()
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

func (r *TxnOffsetCommitV2) GetTopics() []string {
	return r.Topics
}

func (r *TxnOffsetCommitV2) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type TxnOffsetCommitV3 struct {
	Topics         []string
	ConsumerGroups []string
}

func (r *TxnOffsetCommitV3) encode(pe packetEncoder) error {
	return nil
}

func (r *TxnOffsetCommitV3) key() int16 {
	return 28
}

func (r *TxnOffsetCommitV3) version() int16 {
	return 2
}

func (r *TxnOffsetCommitV3) decode(pd packetDecoder) (err error) {
	// transactional_id
	_, err = pd.getString()
	if err != nil {
		return err
	}
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)

	// producer_id
	_, err = pd.getInt64()
	if err != nil {
		return err
	}

	// producer_epoch
	_, err = pd.getInt16()
	if err != nil {
		return err
	}

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

	// get length of topic array
	numTopics, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numTopics; i++ {
		topicName, err := pd.getCompactString()
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
			_, err = pd.getInt64()
			if err != nil {
				return err
			}

			// committed_leader_epoch
			_, err = pd.getInt32()
			if err != nil {
				return err
			}

			// committed_metadata
			_, err = pd.getCompactNullableString()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *TxnOffsetCommitV3) GetTopics() []string {
	return r.Topics
}

func (r *TxnOffsetCommitV3) GetConsumerGroups() []string {
	return r.ConsumerGroups
}
