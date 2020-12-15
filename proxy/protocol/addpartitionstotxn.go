package protocol

import "fmt"

type AddPartitionsToTxnRequestFactory struct{}

func (f *AddPartitionsToTxnRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &AddPartitionsToTxnV0{}, nil
	case 1:
		return &AddPartitionsToTxnV1{}, nil
	default:
		return nil, fmt.Errorf("Not supported fetch request %d", requestKeyVersion.ApiVersion)
	}
}

type AddPartitionsToTxnV0 struct {
	Topics []string
}

func (r *AddPartitionsToTxnV0) encode(pe packetEncoder) error {
	return nil
}

func (r *AddPartitionsToTxnV0) key() int16 {
	return 24
}

func (r *AddPartitionsToTxnV0) version() int16 {
	return 0
}

func (r *AddPartitionsToTxnV0) decode(pd packetDecoder) (err error) {
	// transactional_id
	_, err = pd.getString()
	if err != nil {
		return err
	}
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
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *AddPartitionsToTxnV0) GetTopics() []string {
	return r.Topics
}

type AddPartitionsToTxnV1 struct {
	Topics []string
}

func (r *AddPartitionsToTxnV1) encode(pe packetEncoder) error {
	return nil
}

func (r *AddPartitionsToTxnV1) key() int16 {
	return 24
}

func (r *AddPartitionsToTxnV1) version() int16 {
	return 1
}

func (r *AddPartitionsToTxnV1) decode(pd packetDecoder) (err error) {
	// transactional_id
	_, err = pd.getString()
	if err != nil {
		return err
	}
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
			_, err = pd.getInt32()
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (r *AddPartitionsToTxnV1) GetTopics() []string {
	return r.Topics
}
