package protocol

import (
	"fmt"
)

type RequestV2 struct {
	CorrelationID int32
	ClientID      string
	TaggedFields  TaggedFields
	Body          ProtocolBody
}

func (r *RequestV2) encode(pe packetEncoder) (err error) {
	pe.putInt16(r.Body.key())
	pe.putInt16(r.Body.version())
	pe.putInt32(r.CorrelationID)
	err = pe.putString(r.ClientID)
	if err != nil {
		return err
	}
	err = r.TaggedFields.encode(pe)
	if err != nil {
		return err
	}
	err = r.Body.encode(pe)
	if err != nil {
		return err
	}
	return err
}

func (r *RequestV2) decode(pd packetDecoder) (err error) {
	if r.Body == nil {
		return PacketDecodingError{"unknown body decoder"}
	}
	var key int16
	if key, err = pd.getInt16(); err != nil {
		return err
	}
	var version int16
	if version, err = pd.getInt16(); err != nil {
		return err
	}
	if r.Body.key() != key || r.Body.version() != version {
		return PacketDecodingError{fmt.Sprintf("expected request key,version %d,%d but got %d,%d", r.Body.key(), r.Body.version(), key, version)}
	}
	if r.CorrelationID, err = pd.getInt32(); err != nil {
		return err
	}
	if r.ClientID, err = pd.getString(); err != nil {
		return err
	}
	err = r.TaggedFields.decode(pd)
	if err != nil {
		return err
	}
	return r.Body.decode(pd)
}
