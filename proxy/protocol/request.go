package protocol

type ProtocolBody interface {
	encoder
	key() int16
	version() int16
}

type Request struct {
	CorrelationID int32
	ClientID      string
	Body          ProtocolBody
}

func (r *Request) encode(pe packetEncoder) (err error) {
	pe.putInt16(r.Body.key())
	pe.putInt16(r.Body.version())
	pe.putInt32(r.CorrelationID)
	err = pe.putString(r.ClientID)
	if err != nil {
		return err
	}
	err = r.Body.encode(pe)
	if err != nil {
		return err
	}
	return err
}
