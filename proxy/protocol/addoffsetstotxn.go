package protocol

import "fmt"

type AddOffsetsToTxnRequestFactory struct{}

func (f *AddOffsetsToTxnRequestFactory) Produce(requestKeyVersion *RequestKeyVersion) (req ProtocolBody, err error) {
	switch requestKeyVersion.ApiVersion {
	case 0:
		return &AddOffsetsToTxnV0{}, nil
	case 1:
		return &AddOffsetsToTxnV1{}, nil
	default:
		return nil, fmt.Errorf("Not supported fetch request %d", requestKeyVersion.ApiVersion)
	}
}

type AddOffsetsToTxnV0 struct {
	ConsumerGroups []string
}

func (r *AddOffsetsToTxnV0) encode(pe packetEncoder) error {
	return nil
}

func (r *AddOffsetsToTxnV0) key() int16 {
	return 25
}

func (r *AddOffsetsToTxnV0) version() int16 {
	return 0
}

func (r *AddOffsetsToTxnV0) decode(pd packetDecoder) (err error) {
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

	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)

	return err
}

func (r *AddOffsetsToTxnV0) GetConsumerGroups() []string {
	return r.ConsumerGroups
}

type AddOffsetsToTxnV1 struct {
	ConsumerGroups []string
}

func (r *AddOffsetsToTxnV1) encode(pe packetEncoder) error {
	return nil
}

func (r *AddOffsetsToTxnV1) key() int16 {
	return 25
}

func (r *AddOffsetsToTxnV1) version() int16 {
	return 1
}

func (r *AddOffsetsToTxnV1) decode(pd packetDecoder) (err error) {
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

	// group_id
	groupId, err := pd.getString()
	if err != nil {
		return err
	}

	r.ConsumerGroups = append(r.ConsumerGroups, groupId)

	return err
}

func (r *AddOffsetsToTxnV1) GetConsumerGroups() []string {
	return r.ConsumerGroups
}
