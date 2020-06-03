package protocol

import (
	"fmt"
)

type ProduceRequestFactory struct{}

func (f *ProduceRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &ProduceRequestV0{}, nil
	case 1:
		return &ProduceRequestV1{}, nil
	case 2:
		return &ProduceRequestV2{}, nil
	case 3:
		return &ProduceRequestV3{}, nil
	case 4:
		return &ProduceRequestV4{}, nil
	case 5:
		return &ProduceRequestV5{}, nil
	case 6:
		return &ProduceRequestV6{}, nil
	case 7:
		return &ProduceRequestV7{}, nil
	case 8:
		return &ProduceRequestV8{}, nil
	default:
		return nil, fmt.Errorf("Not supported produce request %d", requestKeyVersion.ApiVersion)
	}
}

type ProduceRequestV0 struct {
	Topics []string
}

func (r *ProduceRequestV0) encode(pe packetEncoder) error {
	return nil
}

func (r *ProduceRequestV0) key() int16 {
	return 0
}

func (r *ProduceRequestV0) version() int16 {
	return 0
}

func (r *ProduceRequestV0) decode(pd packetDecoder) (err error) {
	// RequiredAcks
	_, err = pd.getInt16()
	if err != nil {
		return err
	}
	// Timeout
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
			// messageset size
			msgSetSize, err := pd.getInt32()
			if err != nil {
				return err
			}

			err = pd.skipBytes(int(msgSetSize))
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *ProduceRequestV0) GetTopics() []string {
	return r.Topics
}

type ProduceRequestV1 struct {
	Topics []string
}

func (r *ProduceRequestV1) encode(pe packetEncoder) error {
	return nil
}

func (r *ProduceRequestV1) key() int16 {
	return 0
}

func (r *ProduceRequestV1) version() int16 {
	return 1
}

func (r *ProduceRequestV1) decode(pd packetDecoder) (err error) {
	// RequiredAcks
	_, err = pd.getInt16()
	if err != nil {
		return err
	}
	// Timeout
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
			// messageset size
			msgSetSize, err := pd.getInt32()
			if err != nil {
				return err
			}

			err = pd.skipBytes(int(msgSetSize))
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *ProduceRequestV1) GetTopics() []string {
	return r.Topics
}

type ProduceRequestV2 struct {
	Topics []string
}

func (r *ProduceRequestV2) encode(pe packetEncoder) error {
	return nil
}

func (r *ProduceRequestV2) key() int16 {
	return 0
}

func (r *ProduceRequestV2) version() int16 {
	return 2
}

func (r *ProduceRequestV2) decode(pd packetDecoder) (err error) {
	// RequiredAcks
	_, err = pd.getInt16()
	if err != nil {
		return err
	}
	// Timeout
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
			// messageset size
			msgSetSize, err := pd.getInt32()
			if err != nil {
				return err
			}

			err = pd.skipBytes(int(msgSetSize))
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *ProduceRequestV2) GetTopics() []string {
	return r.Topics
}

type ProduceRequestV3 struct {
	Topics []string
}

func (r *ProduceRequestV3) encode(pe packetEncoder) error {
	return nil
}

func (r *ProduceRequestV3) key() int16 {
	return 0
}

func (r *ProduceRequestV3) version() int16 {
	return 3
}

func (r *ProduceRequestV3) decode(pd packetDecoder) (err error) {
	// transactional_id
	_, err = pd.getNullableString()
	if err != nil {
		return err
	}
	// RequiredAcks
	_, err = pd.getInt16()
	if err != nil {
		return err
	}
	// Timeout
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
			// messageset size
			msgSetSize, err := pd.getInt32()
			if err != nil {
				return err
			}

			err = pd.skipBytes(int(msgSetSize))
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *ProduceRequestV3) GetTopics() []string {
	return r.Topics
}

type ProduceRequestV4 struct {
	Topics []string
}

func (r *ProduceRequestV4) encode(pe packetEncoder) error {
	return nil
}

func (r *ProduceRequestV4) key() int16 {
	return 0
}

func (r *ProduceRequestV4) version() int16 {
	return 4
}

func (r *ProduceRequestV4) decode(pd packetDecoder) (err error) {
	// transactional_id
	_, err = pd.getNullableString()
	if err != nil {
		return err
	}
	// RequiredAcks
	_, err = pd.getInt16()
	if err != nil {
		return err
	}
	// Timeout
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
			// messageset size
			msgSetSize, err := pd.getInt32()
			if err != nil {
				return err
			}

			err = pd.skipBytes(int(msgSetSize))
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *ProduceRequestV4) GetTopics() []string {
	return r.Topics
}

type ProduceRequestV5 struct {
	Topics []string
}

func (r *ProduceRequestV5) encode(pe packetEncoder) error {
	return nil
}

func (r *ProduceRequestV5) key() int16 {
	return 0
}

func (r *ProduceRequestV5) version() int16 {
	return 5
}

func (r *ProduceRequestV5) decode(pd packetDecoder) (err error) {
	// transactional_id
	_, err = pd.getNullableString()
	if err != nil {
		return err
	}
	// RequiredAcks
	_, err = pd.getInt16()
	if err != nil {
		return err
	}
	// Timeout
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
			// messageset size
			msgSetSize, err := pd.getInt32()
			if err != nil {
				return err
			}

			err = pd.skipBytes(int(msgSetSize))
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *ProduceRequestV5) GetTopics() []string {
	return r.Topics
}

type ProduceRequestV6 struct {
	Topics []string
}

func (r *ProduceRequestV6) encode(pe packetEncoder) error {
	return nil
}

func (r *ProduceRequestV6) key() int16 {
	return 0
}

func (r *ProduceRequestV6) version() int16 {
	return 6
}

func (r *ProduceRequestV6) decode(pd packetDecoder) (err error) {
	// transactional_id
	_, err = pd.getNullableString()
	if err != nil {
		return err
	}
	// RequiredAcks
	_, err = pd.getInt16()
	if err != nil {
		return err
	}
	// Timeout
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
			// messageset size
			msgSetSize, err := pd.getInt32()
			if err != nil {
				return err
			}

			err = pd.skipBytes(int(msgSetSize))
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *ProduceRequestV6) GetTopics() []string {
	return r.Topics
}

type ProduceRequestV7 struct {
	Topics []string
}

func (r *ProduceRequestV7) encode(pe packetEncoder) error {
	return nil
}

func (r *ProduceRequestV7) key() int16 {
	return 0
}

func (r *ProduceRequestV7) version() int16 {
	return 7
}

func (r *ProduceRequestV7) decode(pd packetDecoder) (err error) {
	// transactional_id
	_, err = pd.getNullableString()
	if err != nil {
		return err
	}
	// RequiredAcks
	_, err = pd.getInt16()
	if err != nil {
		return err
	}
	// Timeout
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
			// messageset size
			msgSetSize, err := pd.getInt32()
			if err != nil {
				return err
			}

			err = pd.skipBytes(int(msgSetSize))
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *ProduceRequestV7) GetTopics() []string {
	return r.Topics
}

type ProduceRequestV8 struct {
	Topics []string
}

func (r *ProduceRequestV8) encode(pe packetEncoder) error {
	return nil
}

func (r *ProduceRequestV8) key() int16 {
	return 0
}

func (r *ProduceRequestV8) version() int16 {
	return 7
}

func (r *ProduceRequestV8) decode(pd packetDecoder) (err error) {
	// transactional_id
	_, err = pd.getNullableString()
	if err != nil {
		return err
	}
	// RequiredAcks
	_, err = pd.getInt16()
	if err != nil {
		return err
	}
	// Timeout
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
			// messageset size
			msgSetSize, err := pd.getInt32()
			if err != nil {
				return err
			}

			err = pd.skipBytes(int(msgSetSize))
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *ProduceRequestV8) GetTopics() []string {
	return r.Topics
}
