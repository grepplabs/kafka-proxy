package protocol

import "fmt"

type SaslAuthenticateRequestV2 struct {
	SaslAuthBytes   []byte
	RawTaggedFields []rawTaggedField
}

func (r *SaslAuthenticateRequestV2) encode(pe packetEncoder) error {
	if err := pe.putCompactBytes(r.SaslAuthBytes); err != nil {
		return err
	}
	tf := &taggedFields{}
	return tf.encode(pe, r.RawTaggedFields)
}

func (r *SaslAuthenticateRequestV2) decode(pd packetDecoder) (err error) {
	if r.SaslAuthBytes, err = pd.getCompactBytes(); err != nil {
		return err
	}
	tf := &taggedFields{}
	taggedFields, err := tf.decode(pd)
	if err != nil {
		return err
	}
	if rawTaggedFields, ok := taggedFields.([]rawTaggedField); ok {
		r.RawTaggedFields = rawTaggedFields
	} else {
		return PacketDecodingError{fmt.Sprintf("taggedFields type %v", taggedFields)}
	}
	return nil
}

func (r *SaslAuthenticateRequestV2) key() int16 {
	return 36
}

func (r *SaslAuthenticateRequestV2) version() int16 {
	return 2
}

type SaslAuthenticateResponseV2 struct {
	Err               KError
	ErrMsg            *string
	SaslAuthBytes     []byte
	SessionLifetimeMs int64
	RawTaggedFields   []rawTaggedField
}

func (r *SaslAuthenticateResponseV2) encode(pe packetEncoder) error {
	pe.putInt16(int16(r.Err))

	if err := pe.putCompactNullableString(r.ErrMsg); err != nil {
		return err
	}

	if err := pe.putCompactBytes(r.SaslAuthBytes); err != nil {
		return err
	}
	pe.putInt64(r.SessionLifetimeMs)

	tf := &taggedFields{}
	return tf.encode(pe, r.RawTaggedFields)
}

func (r *SaslAuthenticateResponseV2) decode(pd packetDecoder) error {
	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}
	r.Err = KError(kerr)

	if r.ErrMsg, err = pd.getCompactNullableString(); err != nil {
		return err
	}
	if r.SaslAuthBytes, err = pd.getCompactBytes(); err != nil {
		return err
	}
	if r.SessionLifetimeMs, err = pd.getInt64(); err != nil {
		return err
	}
	tf := &taggedFields{}
	taggedFields, err := tf.decode(pd)
	if err != nil {
		return err
	}
	if rawTaggedFields, ok := taggedFields.([]rawTaggedField); ok {
		r.RawTaggedFields = rawTaggedFields
	} else {
		return PacketDecodingError{fmt.Sprintf("taggedFields type %v", taggedFields)}
	}

	return nil
}
