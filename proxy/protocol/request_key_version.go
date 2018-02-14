package protocol

import "fmt"

type RequestKeyVersion struct {
	Length     int32
	ApiKey     int16
	ApiVersion int16
}

func (r *RequestKeyVersion) decode(pd packetDecoder) (err error) {
	r.Length, err = pd.getInt32()
	if err != nil {
		return err
	}
	if r.Length <= 4 {
		return PacketDecodingError{fmt.Sprintf("message of length %d too small", r.Length)}
	}
	r.ApiKey, err = pd.getInt16()
	if err != nil {
		return err
	}
	r.ApiVersion, err = pd.getInt16()
	return err
}
