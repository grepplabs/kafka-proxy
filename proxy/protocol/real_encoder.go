package protocol

import (
	"encoding/binary"
)

type realEncoder struct {
	raw []byte
	off int
}

// primitives

func (re *realEncoder) putInt8(in int8) {
	re.raw[re.off] = byte(in)
	re.off++
}

func (re *realEncoder) putInt16(in int16) {
	binary.BigEndian.PutUint16(re.raw[re.off:], uint16(in))
	re.off += 2
}

func (re *realEncoder) putInt32(in int32) {
	binary.BigEndian.PutUint32(re.raw[re.off:], uint32(in))
	re.off += 4
}

func (re *realEncoder) putInt64(in int64) {
	binary.BigEndian.PutUint64(re.raw[re.off:], uint64(in))
	re.off += 8
}

func (re *realEncoder) putVarint(in int64) {
	re.off += binary.PutUvarint(re.raw[re.off:], uint64(in))
}

func (re *realEncoder) putArrayLength(in int) error {
	re.putInt32(int32(in))
	return nil
}

func (re *realEncoder) putBool(in bool) {
	if in {
		re.putInt8(1)
		return
	}
	re.putInt8(0)
}

// collection

func (re *realEncoder) putRawBytes(in []byte) error {
	copy(re.raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *realEncoder) putBytes(in []byte) error {
	if in == nil {
		re.putInt32(-1)
		return nil
	}
	re.putInt32(int32(len(in)))
	return re.putRawBytes(in)
}

func (re *realEncoder) putCompactBytes(in []byte) error {
	re.putVarint(int64(len(in) + 1))
	copy(re.raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *realEncoder) putVarintBytes(in []byte) error {
	if in == nil {
		re.putVarint(-1)
		return nil
	}
	re.putVarint(int64(len(in)))
	return re.putRawBytes(in)
}

func (re *realEncoder) putString(in string) error {
	re.putInt16(int16(len(in)))
	copy(re.raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *realEncoder) putNullableString(in *string) error {
	if in == nil {
		re.putInt16(-1)
		return nil
	}
	return re.putString(*in)
}

func (re *realEncoder) putStringArray(in []string) error {
	err := re.putArrayLength(len(in))
	if err != nil {
		return err
	}

	for _, val := range in {
		if err := re.putString(val); err != nil {
			return err
		}
	}

	return nil
}

func (re *realEncoder) putInt32Array(in []int32) error {
	err := re.putArrayLength(len(in))
	if err != nil {
		return err
	}
	for _, val := range in {
		re.putInt32(val)
	}
	return nil
}

func (re *realEncoder) putInt64Array(in []int64) error {
	err := re.putArrayLength(len(in))
	if err != nil {
		return err
	}
	for _, val := range in {
		re.putInt64(val)
	}
	return nil
}

func (re *realEncoder) offset() int {
	return re.off
}

func (re *realEncoder) putCompactString(in string) error {
	re.putVarint(int64(len(in) + 1))
	copy(re.raw[re.off:], in)
	re.off += len(in)
	return nil
}

func (re *realEncoder) putCompactNullableString(in *string) error {
	if in == nil {
		re.putVarint(0)
		return nil
	}
	return re.putCompactString(*in)
}

func (pe *realEncoder) putCompactArrayLength(in int) error {
	pe.putVarint(int64(in + 1))
	return nil
}

func (pe *realEncoder) putCompactNullableArrayLength(in int) error {
	pe.putVarint(int64(in + 1))
	return nil
}
