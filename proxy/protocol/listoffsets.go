package protocol

import "fmt"

type ListOffsetsRequestFactory struct{}

func (f *ListOffsetsRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &ListOffsetsRequestV0{}, nil
	case 1:
		return &ListOffsetsRequestV1{}, nil
	case 2:
		return &ListOffsetsRequestV2{}, nil
	case 3:
		return &ListOffsetsRequestV3{}, nil
	case 4:
		return &ListOffsetsRequestV4{}, nil
	case 5:
		return &ListOffsetsRequestV5{}, nil
	default:
		return nil, fmt.Errorf("Not supported listoffsets request %d", requestKeyVersion.ApiVersion)
	}
}

type ListOffsetsRequestV0 struct {
	Topics []string
}

func (r *ListOffsetsRequestV0) encode(pe packetEncoder) error {
	return nil
}

func (r *ListOffsetsRequestV0) key() int16 {
	return 2
}

func (r *ListOffsetsRequestV0) version() int16 {
	return 0
}

func (r *ListOffsetsRequestV0) decode(pd packetDecoder) (err error) {
	// replica_id
	_, err = pd.getInt32()
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
			// partition
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// timestamp
			_, err := pd.getInt64()
			if err != nil {
				return err
			}
			// max_num_offsets
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *ListOffsetsRequestV0) GetTopics() []string {
	return r.Topics
}

type ListOffsetsRequestV1 struct {
	Topics []string
}

func (r *ListOffsetsRequestV1) encode(pe packetEncoder) error {
	return nil
}

func (r *ListOffsetsRequestV1) key() int16 {
	return 2
}

func (r *ListOffsetsRequestV1) version() int16 {
	return 1
}

func (r *ListOffsetsRequestV1) decode(pd packetDecoder) (err error) {
	// replica_id
	_, err = pd.getInt32()
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
			// partition
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// timestamp
			_, err := pd.getInt64()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *ListOffsetsRequestV1) GetTopics() []string {
	return r.Topics
}

type ListOffsetsRequestV2 struct {
	Topics []string
}

func (r *ListOffsetsRequestV2) encode(pe packetEncoder) error {
	return nil
}

func (r *ListOffsetsRequestV2) key() int16 {
	return 2
}

func (r *ListOffsetsRequestV2) version() int16 {
	return 2
}

func (r *ListOffsetsRequestV2) decode(pd packetDecoder) (err error) {
	// replica_id
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// isolation_level
	_, err = pd.getInt8()
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
			// partition
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// timestamp
			_, err := pd.getInt64()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *ListOffsetsRequestV2) GetTopics() []string {
	return r.Topics
}

type ListOffsetsRequestV3 struct {
	Topics []string
}

func (r *ListOffsetsRequestV3) encode(pe packetEncoder) error {
	return nil
}

func (r *ListOffsetsRequestV3) key() int16 {
	return 2
}

func (r *ListOffsetsRequestV3) version() int16 {
	return 3
}

func (r *ListOffsetsRequestV3) decode(pd packetDecoder) (err error) {
	// replica_id
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// isolation_level
	_, err = pd.getInt8()
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
			// partition
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// timestamp
			_, err := pd.getInt64()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *ListOffsetsRequestV3) GetTopics() []string {
	return r.Topics
}

type ListOffsetsRequestV4 struct {
	Topics []string
}

func (r *ListOffsetsRequestV4) encode(pe packetEncoder) error {
	return nil
}

func (r *ListOffsetsRequestV4) key() int16 {
	return 2
}

func (r *ListOffsetsRequestV4) version() int16 {
	return 4
}

func (r *ListOffsetsRequestV4) decode(pd packetDecoder) (err error) {
	// replica_id
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// isolation_level
	_, err = pd.getInt8()
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
			// partition
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			//current_leader_epoch
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// timestamp
			_, err := pd.getInt64()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *ListOffsetsRequestV4) GetTopics() []string {
	return r.Topics
}

type ListOffsetsRequestV5 struct {
	Topics []string
}

func (r *ListOffsetsRequestV5) encode(pe packetEncoder) error {
	return nil
}

func (r *ListOffsetsRequestV5) key() int16 {
	return 2
}

func (r *ListOffsetsRequestV5) version() int16 {
	return 5
}

func (r *ListOffsetsRequestV5) decode(pd packetDecoder) (err error) {
	// replica_id
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// isolation_level
	_, err = pd.getInt8()
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
			// partition
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			//current_leader_epoch
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// timestamp
			_, err := pd.getInt64()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *ListOffsetsRequestV5) GetTopics() []string {
	return r.Topics
}
