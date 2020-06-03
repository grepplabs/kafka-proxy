package protocol

import (
	"encoding/binary"
	"math"
)

var errInvalidArrayLength = PacketDecodingError{"invalid array length"}
var errInvalidStringLength = PacketDecodingError{"invalid string length"}
var errVarintOverflow = PacketDecodingError{"varint overflow"}
var errInvalidBool = PacketDecodingError{"invalid bool"}
var errInvalidByteSliceLength = PacketDecodingError{"invalid byte slice length"}
var errInvalidCompactLength = PacketDecodingError{"invalid compact length"}
var errInvalidCompactNullableLength = PacketDecodingError{"invalid compact nullable length"}

type realDecoder struct {
	raw []byte
	off int
}

// primitives

func (rd *realDecoder) skipBytes(skip int) error {
	if rd.remaining() < skip {
		rd.off = len(rd.raw)
		return ErrInsufficientData
	}
	rd.off += skip
	return nil
}

func (rd *realDecoder) getInt8() (int8, error) {
	if rd.remaining() < 1 {
		rd.off = len(rd.raw)
		return -1, ErrInsufficientData
	}
	tmp := int8(rd.raw[rd.off])
	rd.off++
	return tmp, nil
}

func (rd *realDecoder) getInt16() (int16, error) {
	if rd.remaining() < 2 {
		rd.off = len(rd.raw)
		return -1, ErrInsufficientData
	}
	tmp := int16(binary.BigEndian.Uint16(rd.raw[rd.off:]))
	rd.off += 2
	return tmp, nil
}

func (rd *realDecoder) getInt32() (int32, error) {
	if rd.remaining() < 4 {
		rd.off = len(rd.raw)
		return -1, ErrInsufficientData
	}
	tmp := int32(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4
	return tmp, nil
}

func (rd *realDecoder) getInt64() (int64, error) {
	if rd.remaining() < 8 {
		rd.off = len(rd.raw)
		return -1, ErrInsufficientData
	}
	tmp := int64(binary.BigEndian.Uint64(rd.raw[rd.off:]))
	rd.off += 8
	return tmp, nil
}

func (rd *realDecoder) getVarint() (int64, error) {
	tmp, n := binary.Uvarint(rd.raw[rd.off:])
	if n == 0 {
		rd.off = len(rd.raw)
		return -1, ErrInsufficientData
	}
	if n < 0 {
		rd.off -= n
		return -1, errVarintOverflow
	}
	rd.off += n
	return int64(tmp), nil
}

func (rd *realDecoder) getArrayLength() (int, error) {
	if rd.remaining() < 4 {
		rd.off = len(rd.raw)
		return -1, ErrInsufficientData
	}
	tmp := int(int32(binary.BigEndian.Uint32(rd.raw[rd.off:])))
	rd.off += 4
	if tmp > rd.remaining() {
		rd.off = len(rd.raw)
		return -1, ErrInsufficientData
	} else if tmp > 2*math.MaxUint16 {
		return -1, errInvalidArrayLength
	}
	return tmp, nil
}

func (rd *realDecoder) getBool() (bool, error) {
	b, err := rd.getInt8()
	if err != nil || b == 0 {
		return false, err
	}
	if b != 1 {
		return false, errInvalidBool
	}
	return true, nil
}

// collections

func (rd *realDecoder) getBytes() ([]byte, error) {
	tmp, err := rd.getInt32()
	if err != nil {
		return nil, err
	}
	if tmp == -1 {
		return nil, nil
	}

	return rd.getRawBytes(int(tmp))
}

func (rd *realDecoder) getVarintBytes() ([]byte, error) {
	tmp, err := rd.getVarint()
	if err != nil {
		return nil, err
	}
	if tmp == -1 {
		return nil, nil
	}
	return rd.getRawBytes(int(tmp))
}

func (rd *realDecoder) getRawBytes(length int) ([]byte, error) {
	if length < 0 {
		return nil, errInvalidByteSliceLength
	} else if length > rd.remaining() {
		rd.off = len(rd.raw)
		return nil, ErrInsufficientData
	}

	start := rd.off
	rd.off += length
	return rd.raw[start:rd.off], nil
}

func (rd *realDecoder) getCompactBytes() ([]byte, error) {

	n, err := rd.getCompactLength()
	if err != nil || n == 0 {
		return []byte{}, err
	}
	tmp := rd.raw[rd.off : rd.off+n]
	rd.off += n
	return tmp, nil
}

func (rd *realDecoder) getStringLength() (int, error) {
	length, err := rd.getInt16()
	if err != nil {
		return 0, err
	}

	n := int(length)

	switch {
	case n < -1:
		return 0, errInvalidStringLength
	case n > rd.remaining():
		rd.off = len(rd.raw)
		return 0, ErrInsufficientData
	}

	return n, nil
}

func (rd *realDecoder) getString() (string, error) {
	n, err := rd.getStringLength()
	if err != nil || n == -1 {
		return "", err
	}

	tmpStr := string(rd.raw[rd.off : rd.off+n])
	rd.off += n
	return tmpStr, nil
}

func (rd *realDecoder) getNullableString() (*string, error) {
	n, err := rd.getStringLength()
	if err != nil || n == -1 {
		return nil, err
	}

	tmpStr := string(rd.raw[rd.off : rd.off+n])
	rd.off += n
	return &tmpStr, err
}

func (rd *realDecoder) getInt32Array() ([]int32, error) {
	if rd.remaining() < 4 {
		rd.off = len(rd.raw)
		return nil, ErrInsufficientData
	}
	n := int(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4

	if rd.remaining() < 4*n {
		rd.off = len(rd.raw)
		return nil, ErrInsufficientData
	}

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errInvalidArrayLength
	}

	ret := make([]int32, n)
	for i := range ret {
		ret[i] = int32(binary.BigEndian.Uint32(rd.raw[rd.off:]))
		rd.off += 4
	}
	return ret, nil
}

func (rd *realDecoder) getInt64Array() ([]int64, error) {
	if rd.remaining() < 4 {
		rd.off = len(rd.raw)
		return nil, ErrInsufficientData
	}
	n := int(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4

	if rd.remaining() < 8*n {
		rd.off = len(rd.raw)
		return nil, ErrInsufficientData
	}

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errInvalidArrayLength
	}

	ret := make([]int64, n)
	for i := range ret {
		ret[i] = int64(binary.BigEndian.Uint64(rd.raw[rd.off:]))
		rd.off += 8
	}
	return ret, nil
}

func (rd *realDecoder) getStringArray() ([]string, error) {
	if rd.remaining() < 4 {
		rd.off = len(rd.raw)
		return nil, ErrInsufficientData
	}
	n := int(binary.BigEndian.Uint32(rd.raw[rd.off:]))
	rd.off += 4

	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errInvalidArrayLength
	}

	ret := make([]string, n)
	for i := range ret {
		str, err := rd.getString()
		if err != nil {
			return nil, err
		}

		ret[i] = str
	}
	return ret, nil
}

// subsets
func (rd *realDecoder) remaining() int {
	return len(rd.raw) - rd.off
}

func (rd *realDecoder) getCompactLength() (int, error) {
	length, err := rd.getVarint()
	if err != nil {
		return 0, err
	}
	n := int(length - 1)

	switch {
	case n < 0:
		return 0, errInvalidCompactLength
	case n > rd.remaining():
		rd.off = len(rd.raw)
		return 0, ErrInsufficientData
	}
	return n, nil
}

func (rd *realDecoder) getCompactNullableLength() (int, error) {
	length, err := rd.getVarint()
	if err != nil {
		return 0, err
	}
	n := int(length - 1)

	switch {
	case n < -1:
		return 0, errInvalidCompactNullableLength
	case n > rd.remaining():
		rd.off = len(rd.raw)
		return 0, ErrInsufficientData
	}
	return n, nil
}

func (rd *realDecoder) getCompactString() (string, error) {

	n, err := rd.getCompactLength()
	if err != nil || n == 0 {
		return "", err
	}
	tmpStr := string(rd.raw[rd.off : rd.off+n])
	rd.off += n
	return tmpStr, nil
}

func (rd *realDecoder) getCompactNullableString() (*string, error) {

	n, err := rd.getCompactNullableLength()
	if err != nil || n < 0 {
		return nil, err
	}
	tmpStr := string(rd.raw[rd.off : rd.off+n])
	rd.off += n
	return &tmpStr, nil
}

func (rd *realDecoder) getCompactArrayLength() (int, error) {
	return rd.getCompactLength()
}

func (rd *realDecoder) getCompactNullableArrayLength() (int, error) {
	return rd.getCompactNullableLength()
}
