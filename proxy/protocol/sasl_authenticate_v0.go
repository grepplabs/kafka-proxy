package protocol

type SaslAuthenticateRequestV0 struct {
	SaslAuthBytes []byte
}

func (r *SaslAuthenticateRequestV0) encode(pe packetEncoder) error {
	if err := pe.putBytes(r.SaslAuthBytes); err != nil {
		return err
	}
	return nil
}
func (r *SaslAuthenticateRequestV0) decode(pd packetDecoder) (err error) {
	if r.SaslAuthBytes, err = pd.getBytes(); err != nil {
		return err
	}

	return nil
}

func (r *SaslAuthenticateRequestV0) key() int16 {
	return 36
}

func (r *SaslAuthenticateRequestV0) version() int16 {
	return 0
}

type SaslAuthenticateResponseV0 struct {
	Err           KError
	ErrMsg        *string
	SaslAuthBytes []byte
}

func (r *SaslAuthenticateResponseV0) encode(pe packetEncoder) error {
	pe.putInt16(int16(r.Err))

	if err := pe.putNullableString(r.ErrMsg); err != nil {
		return err
	}
	return pe.putBytes(r.SaslAuthBytes)
}

func (r *SaslAuthenticateResponseV0) decode(pd packetDecoder) error {
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
	return nil
}
