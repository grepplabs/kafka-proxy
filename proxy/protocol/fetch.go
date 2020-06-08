package protocol

import "fmt"

type FetchRequestFactory struct{}

func (f *FetchRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &FetchRequestV0{}, nil
	case 1:
		return &FetchRequestV1{}, nil
	case 2:
		return &FetchRequestV2{}, nil
	case 3:
		return &FetchRequestV3{}, nil
	case 4:
		return &FetchRequestV4{}, nil
	case 5:
		return &FetchRequestV5{}, nil
	case 6:
		return &FetchRequestV6{}, nil
	case 7:
		return &FetchRequestV7{}, nil
	case 8:
		return &FetchRequestV8{}, nil
	case 9:
		return &FetchRequestV9{}, nil
	case 10:
		return &FetchRequestV10{}, nil
	case 11:
		return &FetchRequestV11{}, nil
	default:
		return nil, fmt.Errorf("Not supported fetch request %d", requestKeyVersion.ApiVersion)
	}
}

type FetchRequestV0 struct {
	Topics []string
}

func (r *FetchRequestV0) encode(pe packetEncoder) error {
	return nil
}

func (r *FetchRequestV0) key() int16 {
	return 1
}

func (r *FetchRequestV0) version() int16 {
	return 0
}

func (r *FetchRequestV0) decode(pd packetDecoder) (err error) {
	// reaplicaId
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxWaitTime
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// minBytes
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
			_, err = pd.getInt32()
			if err != nil {
				return err
			}

			_, err = pd.getInt64()
			if err != nil {
				return err
			}

			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *FetchRequestV0) GetTopics() []string {
	return r.Topics
}

type FetchRequestV1 struct {
	Topics []string
}

func (r *FetchRequestV1) encode(pe packetEncoder) error {
	return nil
}

func (r *FetchRequestV1) key() int16 {
	return 1
}

func (r *FetchRequestV1) version() int16 {
	return 1
}

func (r *FetchRequestV1) decode(pd packetDecoder) (err error) {
	// reaplicaId
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxWaitTime
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// minBytes
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
			// fetch_offset
			_, err = pd.getInt64()
			if err != nil {
				return err
			}
			// partition_max_bytes
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *FetchRequestV1) GetTopics() []string {
	return r.Topics
}

type FetchRequestV2 struct {
	Topics []string
}

func (r *FetchRequestV2) encode(pe packetEncoder) error {
	return nil
}

func (r *FetchRequestV2) key() int16 {
	return 1
}

func (r *FetchRequestV2) version() int16 {
	return 2
}

func (r *FetchRequestV2) decode(pd packetDecoder) (err error) {
	// reaplicaId
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxWaitTime
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// minBytes
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
			_, err = pd.getInt32()
			if err != nil {
				return err
			}

			_, err = pd.getInt64()
			if err != nil {
				return err
			}

			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *FetchRequestV2) GetTopics() []string {
	return r.Topics
}

type FetchRequestV3 struct {
	Topics []string
}

func (r *FetchRequestV3) encode(pe packetEncoder) error {
	return nil
}

func (r *FetchRequestV3) key() int16 {
	return 1
}

func (r *FetchRequestV3) version() int16 {
	return 3
}

func (r *FetchRequestV3) decode(pd packetDecoder) (err error) {
	// reaplicaId
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxWaitTime
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// minBytes
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxBytes
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
			_, err = pd.getInt32()
			if err != nil {
				return err
			}

			_, err = pd.getInt64()
			if err != nil {
				return err
			}

			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *FetchRequestV3) GetTopics() []string {
	return r.Topics
}

type FetchRequestV4 struct {
	Topics []string
}

func (r *FetchRequestV4) encode(pe packetEncoder) error {
	return nil
}

func (r *FetchRequestV4) key() int16 {
	return 1
}

func (r *FetchRequestV4) version() int16 {
	return 4
}

func (r *FetchRequestV4) decode(pd packetDecoder) (err error) {
	// reaplicaId
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxWaitTime
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// minBytes
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxBytes
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// isolation level
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
			_, err = pd.getInt32()
			if err != nil {
				return err
			}

			_, err = pd.getInt64()
			if err != nil {
				return err
			}

			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *FetchRequestV4) GetTopics() []string {
	return r.Topics
}

type FetchRequestV5 struct {
	Topics []string
}

func (r *FetchRequestV5) encode(pe packetEncoder) error {
	return nil
}

func (r *FetchRequestV5) key() int16 {
	return 1
}

func (r *FetchRequestV5) version() int16 {
	return 5
}

func (r *FetchRequestV5) decode(pd packetDecoder) (err error) {
	// reaplicaId
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxWaitTime
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// minBytes
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxBytes
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// isolation level
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
			// fetch_offset
			_, err = pd.getInt64()
			if err != nil {
				return err
			}
			// log_start_offset
			_, err = pd.getInt64()
			if err != nil {
				return err
			}
			// partition_max_bytes
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *FetchRequestV5) GetTopics() []string {
	return r.Topics
}

type FetchRequestV6 struct {
	Topics []string
}

func (r *FetchRequestV6) encode(pe packetEncoder) error {
	return nil
}

func (r *FetchRequestV6) key() int16 {
	return 1
}

func (r *FetchRequestV6) version() int16 {
	return 6
}

func (r *FetchRequestV6) decode(pd packetDecoder) (err error) {
	// reaplicaId
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxWaitTime
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// minBytes
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxBytes
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// isolation level
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
			// fetch_offset
			_, err = pd.getInt64()
			if err != nil {
				return err
			}
			// log_start_offset
			_, err = pd.getInt64()
			if err != nil {
				return err
			}
			// partition_max_bytes
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *FetchRequestV6) GetTopics() []string {
	return r.Topics
}

type FetchRequestV7 struct {
	Topics []string
}

func (r *FetchRequestV7) encode(pe packetEncoder) error {
	return nil
}

func (r *FetchRequestV7) key() int16 {
	return 1
}

func (r *FetchRequestV7) version() int16 {
	return 7
}

func (r *FetchRequestV7) decode(pd packetDecoder) (err error) {
	// reaplicaId
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxWaitTime
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// minBytes
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxBytes
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// isolation level
	_, err = pd.getInt8()
	if err != nil {
		return err
	}
	// session_id
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// session_epoch
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
			// fetch_offset
			_, err = pd.getInt64()
			if err != nil {
				return err
			}
			// log_start_offset
			_, err = pd.getInt64()
			if err != nil {
				return err
			}
			// partition_max_bytes
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	// get length of topic array
	forgotNumTopics, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= forgotNumTopics; i++ {
		// forgotten topic name
		_, err := pd.getString()
		if err != nil {
			return err
		}

		// get length of forgot partition array
		forgotNumPart, err := pd.getInt32()
		if err != nil {
			return err
		}

		for i := int32(1); i <= forgotNumPart; i++ {
			// partition
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *FetchRequestV7) GetTopics() []string {
	return r.Topics
}

type FetchRequestV8 struct {
	Topics []string
}

func (r *FetchRequestV8) encode(pe packetEncoder) error {
	return nil
}

func (r *FetchRequestV8) key() int16 {
	return 1
}

func (r *FetchRequestV8) version() int16 {
	return 8
}

func (r *FetchRequestV8) decode(pd packetDecoder) (err error) {
	// reaplicaId
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxWaitTime
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// minBytes
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxBytes
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// isolation level
	_, err = pd.getInt8()
	if err != nil {
		return err
	}
	// session_id
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// session_epoch
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
			// fetch_offset
			_, err = pd.getInt64()
			if err != nil {
				return err
			}
			// log_start_offset
			_, err = pd.getInt64()
			if err != nil {
				return err
			}
			// partition_max_bytes
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	// get length of topic array
	forgotNumTopics, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= forgotNumTopics; i++ {
		// forgotten topic name
		_, err := pd.getString()
		if err != nil {
			return err
		}

		// get length of forgot partition array
		forgotNumPart, err := pd.getInt32()
		if err != nil {
			return err
		}

		for i := int32(1); i <= forgotNumPart; i++ {
			// partition
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *FetchRequestV8) GetTopics() []string {
	return r.Topics
}

type FetchRequestV9 struct {
	Topics []string
}

func (r *FetchRequestV9) encode(pe packetEncoder) error {
	return nil
}

func (r *FetchRequestV9) key() int16 {
	return 1
}

func (r *FetchRequestV9) version() int16 {
	return 9
}

func (r *FetchRequestV9) decode(pd packetDecoder) (err error) {
	// reaplicaId
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxWaitTime
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// minBytes
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxBytes
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// isolation level
	_, err = pd.getInt8()
	if err != nil {
		return err
	}
	// session_id
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// session_epoch
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
			// current_leader_epoch
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// fetch_offset
			_, err = pd.getInt64()
			if err != nil {
				return err
			}
			// log_start_offset
			_, err = pd.getInt64()
			if err != nil {
				return err
			}
			// partition_max_bytes
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	// get length of topic array
	forgotNumTopics, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= forgotNumTopics; i++ {
		// forgotten topic name
		_, err := pd.getString()
		if err != nil {
			return err
		}

		// get length of forgot partition array
		forgotNumPart, err := pd.getInt32()
		if err != nil {
			return err
		}

		for i := int32(1); i <= forgotNumPart; i++ {
			// partition
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *FetchRequestV9) GetTopics() []string {
	return r.Topics
}

type FetchRequestV10 struct {
	Topics []string
}

func (r *FetchRequestV10) encode(pe packetEncoder) error {
	return nil
}

func (r *FetchRequestV10) key() int16 {
	return 1
}

func (r *FetchRequestV10) version() int16 {
	return 10
}

func (r *FetchRequestV10) decode(pd packetDecoder) (err error) {
	// reaplicaId
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxWaitTime
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// minBytes
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxBytes
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// isolation level
	_, err = pd.getInt8()
	if err != nil {
		return err
	}
	// session_id
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// session_epoch
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
			// current_leader_epoch
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// fetch_offset
			_, err = pd.getInt64()
			if err != nil {
				return err
			}
			// log_start_offset
			_, err = pd.getInt64()
			if err != nil {
				return err
			}
			// partition_max_bytes
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	// get length of topic array
	forgotNumTopics, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= forgotNumTopics; i++ {
		// forgotten topic name
		_, err := pd.getString()
		if err != nil {
			return err
		}

		// get length of forgot partition array
		forgotNumPart, err := pd.getInt32()
		if err != nil {
			return err
		}

		for i := int32(1); i <= forgotNumPart; i++ {
			// partition
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *FetchRequestV10) GetTopics() []string {
	return r.Topics
}

type FetchRequestV11 struct {
	Topics []string
}

func (r *FetchRequestV11) encode(pe packetEncoder) error {
	return nil
}

func (r *FetchRequestV11) key() int16 {
	return 1
}

func (r *FetchRequestV11) version() int16 {
	return 11
}

func (r *FetchRequestV11) decode(pd packetDecoder) (err error) {
	// reaplicaId
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxWaitTime
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// minBytes
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// maxBytes
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// isolation level
	_, err = pd.getInt8()
	if err != nil {
		return err
	}
	// session_id
	_, err = pd.getInt32()
	if err != nil {
		return err
	}
	// session_epoch
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
			// current_leader_epoch
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
			// fetch_offset
			_, err = pd.getInt64()
			if err != nil {
				return err
			}
			// log_start_offset
			_, err = pd.getInt64()
			if err != nil {
				return err
			}
			// partition_max_bytes
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	// get length of topic array
	forgotNumTopics, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= forgotNumTopics; i++ {
		// forgotten topic name
		_, err := pd.getString()
		if err != nil {
			return err
		}

		// get length of forgot partition array
		forgotNumPart, err := pd.getInt32()
		if err != nil {
			return err
		}

		for i := int32(1); i <= forgotNumPart; i++ {
			// partition
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	// rack_id
	_, err = pd.getString()
	if err != nil {
		return err
	}

	return err
}

func (r *FetchRequestV11) GetTopics() []string {
	return r.Topics
}
