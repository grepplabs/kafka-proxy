package protocol

import "fmt"

type WriteTxnMarkersRequestFactory struct{}

func (f *WriteTxnMarkersRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &WriteTxnMarkersV0{}, nil
	default:
		return nil, fmt.Errorf("Not supported fetch request %d", requestKeyVersion.ApiVersion)
	}
}

type WriteTxnMarkersV0 struct {
	Topics []string
}

func (r *WriteTxnMarkersV0) encode(pe packetEncoder) error {
	return nil
}

func (r *WriteTxnMarkersV0) key() int16 {
	return 27
}

func (r *WriteTxnMarkersV0) version() int16 {
	return 0
}

func (r *WriteTxnMarkersV0) decode(pd packetDecoder) (err error) {
	// get length of markers array
	numMarkers, err := pd.getInt32()
	if err != nil {
		return err
	}

	for i := int32(1); i <= numMarkers; i++ {
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
		// transaction_result
		_, err = pd.getBool()
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

		// coordinator_epoch
		_, err = pd.getInt32()
		if err != nil {
			return err
		}
	}

	return err
}

func (r *WriteTxnMarkersV0) GetTopics() []string {
	return r.Topics
}
