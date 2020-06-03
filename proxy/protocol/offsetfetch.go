package protocol

import "fmt"

type OffsetFetchRequestFactory struct{}

func (f *OffsetFetchRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &OffsetFetchRequestV0{}, nil
	case 1:
		return &OffsetFetchRequestV1{}, nil
	case 2:
		return &OffsetFetchRequestV2{}, nil
	case 3:
		return &OffsetFetchRequestV3{}, nil
	case 4:
		return &OffsetFetchRequestV4{}, nil
	case 5:
		return &OffsetFetchRequestV5{}, nil
	default:
		return nil, fmt.Errorf("Not supported listoffsets request %d", requestKeyVersion.ApiVersion)
	}
}

type OffsetFetchRequestV0 struct {
	Topics []string
}

func (r *OffsetFetchRequestV0) encode(pe packetEncoder) error {
	return nil
}

func (r *OffsetFetchRequestV0) key() int16 {
	return 9
}

func (r *OffsetFetchRequestV0) version() int16 {
	return 0
}

func (r *OffsetFetchRequestV0) decode(pd packetDecoder) (err error) {
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
		}
	}

	return err
}

func (r *OffsetFetchRequestV0) GetTopics() []string {
	return r.Topics
}

type OffsetFetchRequestV1 struct {
	Topics []string
}

func (r *OffsetFetchRequestV1) encode(pe packetEncoder) error {
	return nil
}

func (r *OffsetFetchRequestV1) key() int16 {
	return 9
}

func (r *OffsetFetchRequestV1) version() int16 {
	return 1
}

func (r *OffsetFetchRequestV1) decode(pd packetDecoder) (err error) {
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
		}
	}

	return err
}

func (r *OffsetFetchRequestV1) GetTopics() []string {
	return r.Topics
}

type OffsetFetchRequestV2 struct {
	Topics []string
}

func (r *OffsetFetchRequestV2) encode(pe packetEncoder) error {
	return nil
}

func (r *OffsetFetchRequestV2) key() int16 {
	return 9
}

func (r *OffsetFetchRequestV2) version() int16 {
	return 2
}

func (r *OffsetFetchRequestV2) decode(pd packetDecoder) (err error) {
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
		}
	}

	return err
}

func (r *OffsetFetchRequestV2) GetTopics() []string {
	return r.Topics
}

type OffsetFetchRequestV3 struct {
	Topics []string
}

func (r *OffsetFetchRequestV3) encode(pe packetEncoder) error {
	return nil
}

func (r *OffsetFetchRequestV3) key() int16 {
	return 9
}

func (r *OffsetFetchRequestV3) version() int16 {
	return 3
}

func (r *OffsetFetchRequestV3) decode(pd packetDecoder) (err error) {
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
		}
	}

	return err
}

func (r *OffsetFetchRequestV3) GetTopics() []string {
	return r.Topics
}

type OffsetFetchRequestV4 struct {
	Topics []string
}

func (r *OffsetFetchRequestV4) encode(pe packetEncoder) error {
	return nil
}

func (r *OffsetFetchRequestV4) key() int16 {
	return 9
}

func (r *OffsetFetchRequestV4) version() int16 {
	return 4
}

func (r *OffsetFetchRequestV4) decode(pd packetDecoder) (err error) {
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
		}
	}

	return err
}

func (r *OffsetFetchRequestV4) GetTopics() []string {
	return r.Topics
}

type OffsetFetchRequestV5 struct {
	Topics []string
}

func (r *OffsetFetchRequestV5) encode(pe packetEncoder) error {
	return nil
}

func (r *OffsetFetchRequestV5) key() int16 {
	return 9
}

func (r *OffsetFetchRequestV5) version() int16 {
	return 5
}

func (r *OffsetFetchRequestV5) decode(pd packetDecoder) (err error) {
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
		}
	}

	return err
}

func (r *OffsetFetchRequestV5) GetTopics() []string {
	return r.Topics
}
