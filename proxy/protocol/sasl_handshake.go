package protocol

import "github.com/pkg/errors"

type SaslHandshakeRequestV0orV1 struct {
	Version   int16 // not encoded / decoded
	Mechanism string
}

func (r *SaslHandshakeRequestV0orV1) encode(pe packetEncoder) error {
	if r.Version != 0 && r.Version != 1 {
		return errors.New("SaslHandshakeRequestV0orV1 expects version 0 or 1")
	}

	if err := pe.putString(r.Mechanism); err != nil {
		return err
	}
	return nil
}
func (r *SaslHandshakeRequestV0orV1) decode(pd packetDecoder) (err error) {
	if r.Version != 0 && r.Version != 1 {
		return errors.New("SaslHandshakeRequestV0orV1 expects version 0 or 1")
	}
	if r.Mechanism, err = pd.getString(); err != nil {
		return err
	}

	return nil
}

func (r *SaslHandshakeRequestV0orV1) key() int16 {
	return 17
}

func (r *SaslHandshakeRequestV0orV1) version() int16 {
	return r.Version
}

type SaslHandshakeResponseV0orV1 struct {
	Err               KError
	EnabledMechanisms []string
}

func (r *SaslHandshakeResponseV0orV1) encode(pe packetEncoder) error {
	pe.putInt16(int16(r.Err))
	return pe.putStringArray(r.EnabledMechanisms)
}

func (r *SaslHandshakeResponseV0orV1) decode(pd packetDecoder) error {
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
