package protocol

import (
	"encoding/binary"
	"io"
)

type RequestAcksReader struct {
}

func (r RequestAcksReader) readAndDiscardNullableString(reader io.Reader) (err error) {
	var length int16
	if err = binary.Read(reader, binary.BigEndian, &length); err != nil {
		return err
	}
	if length < -1 {
		return errInvalidStringLength
	}
	if length > 0 {
		if _, err = io.CopyN(io.Discard, reader, int64(length)); err != nil {
			return err
		}
	}
	return nil
}

func (r RequestAcksReader) ReadAndDiscardHeaderV1Part(reader io.Reader) (err error) {
	// CorrelationID int32
	var correlationID int32
	if err = binary.Read(reader, binary.BigEndian, &correlationID); err != nil {
		return err
	}
	// ClientID *string
	if err = r.readAndDiscardNullableString(reader); err != nil {
		return err
	}
	return nil
}

func (r RequestAcksReader) ReadAndDiscardProduceAcks(reader io.Reader) (acks int16, err error) {
	// Acks int16
	if err = binary.Read(reader, binary.BigEndian, &acks); err != nil {
		return 0, err
	}
	return acks, nil
}

func (r RequestAcksReader) ReadAndDiscardProduceTxnAcks(reader io.Reader) (acks int16, err error) {
	// TransactionalId *string
	if err = r.readAndDiscardNullableString(reader); err != nil {
		return 0, err
	}

	// Acks int16
	if err = binary.Read(reader, binary.BigEndian, &acks); err != nil {
		return 0, err
	}
	return acks, nil
}
