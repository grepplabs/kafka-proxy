package protocol

import "fmt"

type CreateTopicsRequestFactory struct{}

func (f *CreateTopicsRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &CreateTopicsRequestV0{}, nil
	case 1:
		return &CreateTopicsRequestV1{}, nil
	case 2:
		return &CreateTopicsRequestV2{}, nil
	case 3:
		return &CreateTopicsRequestV3{}, nil
	case 4:
		return &CreateTopicsRequestV4{}, nil
	default:
		return nil, fmt.Errorf("Not supported fetch request %d", requestKeyVersion.ApiVersion)
	}
}

type CreateTopicsRequestV0 struct {
	Topics []string
}

func (r *CreateTopicsRequestV0) encode(pe packetEncoder) error {
	return nil
}

func (r *CreateTopicsRequestV0) key() int16 {
	return 19
}

func (r *CreateTopicsRequestV0) version() int16 {
	return 0
}

func (r *CreateTopicsRequestV0) decode(pd packetDecoder) (err error) {
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

		// num_partitions
		_, err = pd.getInt32()
		if err != nil {
			return err
		}

		// replication_factor
		_, err = pd.getInt16()
		if err != nil {
			return err
		}

		// get length of assignments array
		numAssignments, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numAssignments; j++ {
			// partition_index
			_, err = pd.getInt32()
			if err != nil {
				return err
			}

			// get length of broker ids array
			numBrokers, err := pd.getInt32()
			if err != nil {
				return err
			}

			for j := int32(1); j <= numBrokers; j++ {
				// broker_ids
				_, err = pd.getInt32()
				if err != nil {
					return err
				}
			}
		}

		// get length of configs array
		numConfigs, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numConfigs; j++ {
			// name
			_, err = pd.getString()
			if err != nil {
				return err
			}

			// value
			_, err = pd.getNullableString()
			if err != nil {
				return err
			}
		}
	}

	// timeout_ms
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	return err
}

func (r *CreateTopicsRequestV0) GetTopics() []string {
	return r.Topics
}

type CreateTopicsRequestV1 struct {
	Topics []string
}

func (r *CreateTopicsRequestV1) encode(pe packetEncoder) error {
	return nil
}

func (r *CreateTopicsRequestV1) key() int16 {
	return 19
}

func (r *CreateTopicsRequestV1) version() int16 {
	return 1
}

func (r *CreateTopicsRequestV1) decode(pd packetDecoder) (err error) {
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

		// num_partitions
		_, err = pd.getInt32()
		if err != nil {
			return err
		}

		// replication_factor
		_, err = pd.getInt16()
		if err != nil {
			return err
		}

		// get length of assignments array
		numAssignments, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numAssignments; j++ {
			// partition_index
			_, err = pd.getInt32()
			if err != nil {
				return err
			}

			// get length of broker ids array
			numBrokers, err := pd.getInt32()
			if err != nil {
				return err
			}

			for j := int32(1); j <= numBrokers; j++ {
				// broker_ids
				_, err = pd.getInt32()
				if err != nil {
					return err
				}
			}
		}

		// get length of configs array
		numConfigs, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numConfigs; j++ {
			// name
			_, err = pd.getString()
			if err != nil {
				return err
			}

			// value
			_, err = pd.getNullableString()
			if err != nil {
				return err
			}
		}
	}

	// timeout_ms
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// validate_only
	_, err = pd.getBool()
	if err != nil {
		return err
	}

	return err
}

func (r *CreateTopicsRequestV1) GetTopics() []string {
	return r.Topics
}

type CreateTopicsRequestV2 struct {
	Topics []string
}

func (r *CreateTopicsRequestV2) encode(pe packetEncoder) error {
	return nil
}

func (r *CreateTopicsRequestV2) key() int16 {
	return 19
}

func (r *CreateTopicsRequestV2) version() int16 {
	return 2
}

func (r *CreateTopicsRequestV2) decode(pd packetDecoder) (err error) {
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

		// num_partitions
		_, err = pd.getInt32()
		if err != nil {
			return err
		}

		// replication_factor
		_, err = pd.getInt16()
		if err != nil {
			return err
		}

		// get length of assignments array
		numAssignments, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numAssignments; j++ {
			// partition_index
			_, err = pd.getInt32()
			if err != nil {
				return err
			}

			// get length of broker ids array
			numBrokers, err := pd.getInt32()
			if err != nil {
				return err
			}

			for j := int32(1); j <= numBrokers; j++ {
				// broker_ids
				_, err = pd.getInt32()
				if err != nil {
					return err
				}
			}
		}

		// get length of configs array
		numConfigs, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numConfigs; j++ {
			// name
			_, err = pd.getString()
			if err != nil {
				return err
			}

			// value
			_, err = pd.getNullableString()
			if err != nil {
				return err
			}
		}
	}

	// timeout_ms
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// validate_only
	_, err = pd.getBool()
	if err != nil {
		return err
	}

	return err
}

func (r *CreateTopicsRequestV2) GetTopics() []string {
	return r.Topics
}

type CreateTopicsRequestV3 struct {
	Topics []string
}

func (r *CreateTopicsRequestV3) encode(pe packetEncoder) error {
	return nil
}

func (r *CreateTopicsRequestV3) key() int16 {
	return 19
}

func (r *CreateTopicsRequestV3) version() int16 {
	return 3
}

func (r *CreateTopicsRequestV3) decode(pd packetDecoder) (err error) {
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

		// num_partitions
		_, err = pd.getInt32()
		if err != nil {
			return err
		}

		// replication_factor
		_, err = pd.getInt16()
		if err != nil {
			return err
		}

		// get length of assignments array
		numAssignments, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numAssignments; j++ {
			// partition_index
			_, err = pd.getInt32()
			if err != nil {
				return err
			}

			// get length of broker ids array
			numBrokers, err := pd.getInt32()
			if err != nil {
				return err
			}

			for j := int32(1); j <= numBrokers; j++ {
				// broker_ids
				_, err = pd.getInt32()
				if err != nil {
					return err
				}
			}
		}

		// get length of configs array
		numConfigs, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numConfigs; j++ {
			// name
			_, err = pd.getString()
			if err != nil {
				return err
			}

			// value
			_, err = pd.getNullableString()
			if err != nil {
				return err
			}
		}
	}

	// timeout_ms
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// validate_only
	_, err = pd.getBool()
	if err != nil {
		return err
	}

	return err
}

func (r *CreateTopicsRequestV3) GetTopics() []string {
	return r.Topics
}

type CreateTopicsRequestV4 struct {
	Topics []string
}

func (r *CreateTopicsRequestV4) encode(pe packetEncoder) error {
	return nil
}

func (r *CreateTopicsRequestV4) key() int16 {
	return 19
}

func (r *CreateTopicsRequestV4) version() int16 {
	return 4
}

func (r *CreateTopicsRequestV4) decode(pd packetDecoder) (err error) {
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

		// num_partitions
		_, err = pd.getInt32()
		if err != nil {
			return err
		}

		// replication_factor
		_, err = pd.getInt16()
		if err != nil {
			return err
		}

		// get length of assignments array
		numAssignments, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numAssignments; j++ {
			// partition_index
			_, err = pd.getInt32()
			if err != nil {
				return err
			}

			// get length of broker ids array
			numBrokers, err := pd.getInt32()
			if err != nil {
				return err
			}

			for j := int32(1); j <= numBrokers; j++ {
				// broker_ids
				_, err = pd.getInt32()
				if err != nil {
					return err
				}
			}
		}

		// get length of configs array
		numConfigs, err := pd.getInt32()
		if err != nil {
			return err
		}

		for j := int32(1); j <= numConfigs; j++ {
			// name
			_, err = pd.getString()
			if err != nil {
				return err
			}

			// value
			_, err = pd.getNullableString()
			if err != nil {
				return err
			}
		}
	}

	// timeout_ms
	_, err = pd.getInt32()
	if err != nil {
		return err
	}

	// validate_only
	_, err = pd.getBool()
	if err != nil {
		return err
	}

	return err
}

func (r *CreateTopicsRequestV4) GetTopics() []string {
	return r.Topics
}
