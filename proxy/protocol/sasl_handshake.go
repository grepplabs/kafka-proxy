package protocol

type SaslHandshakeRequestV0 struct {
	Mechanism string
}

func (r *SaslHandshakeRequestV0) encode(pe packetEncoder) error {
	if err := pe.putString(r.Mechanism); err != nil {
		return err
	}
	return nil
}
func (r *SaslHandshakeRequestV0) key() int16 {
	return 17
}

func (r *SaslHandshakeRequestV0) version() int16 {
	return 0
}

type SaslHandshakeResponseV0 struct {
	Err               KError
	EnabledMechanisms []string
}

func (r *SaslHandshakeResponseV0) decode(pd packetDecoder) error {
	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}
	r.Err = KError(kerr)
	if r.EnabledMechanisms, err = pd.getStringArray(); err != nil {
		return err
	}
	return nil
}
