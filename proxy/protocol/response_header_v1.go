package protocol

import (
	"fmt"
)

type ResponseHeaderV1 struct {
	Length          int32
	CorrelationID   int32
	RawTaggedFields []rawTaggedField
}

func (r *ResponseHeaderV1) decode(pd packetDecoder) (err error) {
	r.Length, err = pd.getInt32()
	if err != nil {
		return err
	}
	if r.Length <= 4 {
		return PacketDecodingError{fmt.Sprintf("message of length %d too small", r.Length)}
	}
	r.CorrelationID, err = pd.getInt32()
	if err != nil {
		return err
	}
	tf := &SchemaTaggedFields{}
	taggedFields, err := tf.decode(pd)
	if err != nil {
		return err
	}
	if rawTaggedFields, ok := taggedFields.([]rawTaggedField); ok {
		r.RawTaggedFields = rawTaggedFields
	} else {
		return PacketDecodingError{fmt.Sprintf("taggedFields type %v", taggedFields)}
	}
	return err
}

func (r *ResponseHeaderV1) encode(pe packetEncoder) (err error) {
	pe.putInt32(r.Length)
	pe.putInt32(r.CorrelationID)

	tf := &SchemaTaggedFields{}
	err = tf.encode(pe, r.RawTaggedFields)
	if err != nil {
		return err
	}
	return nil
}
