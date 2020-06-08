package protocol

import (
	"encoding/binary"
	"fmt"
	"math"
)

type prepEncoder struct {
	length int
}

// primitives

func (pe *prepEncoder) putInt8(in int8) {
	pe.length++
}

func (pe *prepEncoder) putInt16(in int16) {
	pe.length += 2
}

func (pe *prepEncoder) putInt32(in int32) {
	pe.length += 4
}

func (pe *prepEncoder) putInt64(in int64) {
	pe.length += 8
}

func (pe *prepEncoder) putVarint(in int64) {
	var buf [binary.MaxVarintLen64]byte
	pe.length += binary.PutUvarint(buf[:], uint64(in))
}

func (pe *prepEncoder) putArrayLength(in int) error {
	if in > math.MaxInt32 {
		return PacketEncodingError{fmt.Sprintf("array too long (%d)", in)}
	}
	pe.length += 4
	return nil
}

func (pe *prepEncoder) putBool(in bool) {
	pe.length++
}

// arrays
func (pe *prepEncoder) putBytes(in []byte) error {
	pe.length += 4
	if in == nil {
		return nil
	}
	return pe.putRawBytes(in)
}

func (pe *prepEncoder) putVarintBytes(in []byte) error {
	if in == nil {
		pe.putVarint(-1)
		return nil
	}
	pe.putVarint(int64(len(in)))
	return pe.putRawBytes(in)
}

func (pe *prepEncoder) putRawBytes(in []byte) error {
	if len(in) > math.MaxInt32 {
		return PacketEncodingError{fmt.Sprintf("byteslice too long (%d)", len(in))}
	}
	pe.length += len(in)
	return nil
}

func (pe *prepEncoder) putCompactBytes(in []byte) error {
	pe.putVarint(int64(len(in) + 1))
	if len(in) > math.MaxInt16 {
		return PacketEncodingError{fmt.Sprintf("compact bytes too long (%d)", len(in))}
	}
	pe.length += len(in)
	return nil
}

func (pe *prepEncoder) putNullableString(in *string) error {
	if in == nil {
		pe.length += 2
		return nil
	}
	return pe.putString(*in)
}

func (pe *prepEncoder) putString(in string) error {
	pe.length += 2
	if len(in) > math.MaxInt16 {
		return PacketEncodingError{fmt.Sprintf("string too long (%d)", len(in))}
	}
	pe.length += len(in)
	return nil
}

func (pe *prepEncoder) putVarintString(in string) error {
	pe.length += 8
	if len(in) > math.MaxInt64 {
		return PacketEncodingError{fmt.Sprintf("string too long (%d)", len(in))}
	}
	pe.length += len(in)
	return nil
}

func (pe *prepEncoder) putStringArray(in []string) error {
	err := pe.putArrayLength(len(in))
	if err != nil {
		return err
	}

	for _, str := range in {
		if err := pe.putString(str); err != nil {
			return err
		}
	}

	return nil
}

func (pe *prepEncoder) putInt32Array(in []int32) error {
	err := pe.putArrayLength(len(in))
	if err != nil {
		return err
	}
	pe.length += 4 * len(in)
	return nil
}

func (pe *prepEncoder) putInt64Array(in []int64) error {
	err := pe.putArrayLength(len(in))
	if err != nil {
		return err
	}
	pe.length += 8 * len(in)
	return nil
}

func (pe *prepEncoder) offset() int {
	return pe.length
}

func (pe *prepEncoder) putCompactString(in string) error {
	pe.putVarint(int64(len(in) + 1))
	if len(in) > math.MaxInt16 {
		return PacketEncodingError{fmt.Sprintf("string too long (%d)", len(in))}
	}
	pe.length += len(in)
	return nil
}

func (pe *prepEncoder) putCompactNullableString(in *string) error {
	if in == nil {
		// A null string is represented with a length of 0.
		pe.length += 1 // pe.putVarint(0) is always 1
		return nil
	}
	return pe.putCompactString(*in)
}

func (pe *prepEncoder) putCompactArrayLength(in int) error {
	switch {
	case in > math.MaxInt16:
		return PacketEncodingError{fmt.Sprintf("comact array too long (%d)", in)}
	case in == -1:
		return PacketEncodingError{fmt.Sprintf("comact array is null (%d)", in)}
	case in < -1:
		return PacketEncodingError{fmt.Sprintf("comact array invalid length (%d)", in)}
	}
	pe.putVarint(int64(in + 1))
	return nil
}

func (pe *prepEncoder) putCompactNullableArrayLength(in int) error {
	switch {
	case in > math.MaxInt16:
		return PacketEncodingError{fmt.Sprintf("comact array too long (%d)", in)}
	case in < -1:
		return PacketEncodingError{fmt.Sprintf("comact array invalid length (%d)", in)}
	}
	pe.putVarint(int64(in + 1))
	return nil
}
