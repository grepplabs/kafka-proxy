package protocol

type SaslAuthenticateRequestV2 struct {
	SaslAuthBytes []byte
	TaggedFields  TaggedFields
}

func (r *SaslAuthenticateRequestV2) encode(pe packetEncoder) error {
	if err := pe.putCompactBytes(r.SaslAuthBytes); err != nil {
		return err
	}
	return r.TaggedFields.encode(pe)
}

func (r *SaslAuthenticateRequestV2) decode(pd packetDecoder) (err error) {
	if r.SaslAuthBytes, err = pd.getCompactBytes(); err != nil {
		return err
	}
	return r.TaggedFields.decode(pd)
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
	TaggedFields      TaggedFields
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

	return r.TaggedFields.encode(pe)
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
	return r.TaggedFields.decode(pd)
}
