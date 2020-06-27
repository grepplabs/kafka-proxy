package protocol

import "fmt"

type TaggedFields struct {
	values []rawTaggedField
}

func (r *TaggedFields) decode(pd packetDecoder) (err error) {
	tf := &taggedFields{}
	taggedFields, err := tf.decode(pd)
	if err != nil {
		return err
	}
	if rawTaggedFields, ok := taggedFields.([]rawTaggedField); ok {
		r.values = rawTaggedFields
	} else {
		return PacketDecodingError{fmt.Sprintf("taggedFields type %v", taggedFields)}
	}
	return err
}

func (r *TaggedFields) encode(pe packetEncoder) (err error) {
	tf := &taggedFields{}
	err = tf.encode(pe, r.values)
	if err != nil {
		return err
	}
	return nil
}
