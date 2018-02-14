package protocol

import (
	"fmt"
)

type ResponseHeader struct {
	Length        int32
	CorrelationID int32
}

func (r *ResponseHeader) decode(pd packetDecoder) (err error) {
	r.Length, err = pd.getInt32()
	if err != nil {
		return err
	}
	if r.Length <= 4 {
		return PacketDecodingError{fmt.Sprintf("message of length %d too small", r.Length)}
	}

	r.CorrelationID, err = pd.getInt32()
	return err
}

func (r *ResponseHeader) encode(pe packetEncoder) (err error) {
	pe.putInt32(r.Length)
	pe.putInt32(r.CorrelationID)
	return nil
}
