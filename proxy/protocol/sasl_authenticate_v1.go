package protocol

type SaslAuthenticateRequestV1 struct {
	SaslAuthBytes []byte
}

func (r *SaslAuthenticateRequestV1) encode(pe packetEncoder) error {
	if err := pe.putBytes(r.SaslAuthBytes); err != nil {
		return err
	}
	return nil
}
func (r *SaslAuthenticateRequestV1) decode(pd packetDecoder) (err error) {
	if r.SaslAuthBytes, err = pd.getBytes(); err != nil {
		return err
	}

	return nil
}

func (r *SaslAuthenticateRequestV1) key() int16 {
	return 36
}

func (r *SaslAuthenticateRequestV1) version() int16 {
	return 1
}

type SaslAuthenticateResponseV1 struct {
	Err               KError
	ErrMsg            *string
	SaslAuthBytes     []byte
	SessionLifetimeMs int64
}

func (r *SaslAuthenticateResponseV1) encode(pe packetEncoder) error {
	pe.putInt16(int16(r.Err))

	if err := pe.putNullableString(r.ErrMsg); err != nil {
		return err
	}

	if err := pe.putBytes(r.SaslAuthBytes); err != nil {
		return err
	}
	pe.putInt64(r.SessionLifetimeMs)

	return nil
}

func (r *SaslAuthenticateResponseV1) decode(pd packetDecoder) error {
	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}
	r.Err = KError(kerr)

	if r.ErrMsg, err = pd.getNullableString(); err != nil {
		return err
	}
	if r.SaslAuthBytes, err = pd.getBytes(); err != nil {
		return err
	}
	if r.SessionLifetimeMs, err = pd.getInt64(); err != nil {
		return err
	}
	return nil
}
