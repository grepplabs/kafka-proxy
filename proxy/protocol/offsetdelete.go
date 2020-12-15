package protocol

import "fmt"

type OffsetDeleteRequestFactory struct{}

func (f *OffsetDeleteRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &OffsetDeleteRequestV0{}, nil
	default:
		return nil, fmt.Errorf("Not supported listoffsets request %d", requestKeyVersion.ApiVersion)
	}
}

type OffsetDeleteRequestV0 struct {
	ConsumerGroups []string
	Topics         []string
}

func (r *OffsetDeleteRequestV0) encode(pe packetEncoder) error {
	return nil
}

func (r *OffsetDeleteRequestV0) key() int16 {
	return 47
}

func (r *OffsetDeleteRequestV0) version() int16 {
	return 0
}

func (r *OffsetDeleteRequestV0) decode(pd packetDecoder) (err error) {
	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)

	// get length of topics array
	numTopics, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numTopics; i++ {
		// name
		topicName, err := pd.getString()
		if err != nil {
			return err
		}

		r.Topics = append(r.Topics, topicName)

		// get length of partitions array
		numPartitions, err := pd.getInt32()
		if err != nil {
			return err
		}

		for i := int32(1); i <= numPartitions; i++ {
			// partition_index
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *OffsetDeleteRequestV0) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

func (r *OffsetDeleteRequestV0) GetTopics() []string {
	return r.Topics
}
