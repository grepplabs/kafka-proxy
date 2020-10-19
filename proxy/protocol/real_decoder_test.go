package protocol

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetInt32Array(t *testing.T) {
	tt := []struct {
		name   string
		raw    string
		values []int32
		err    error
	}{
		{name: "nil array", raw: "FFFFFFFF", values: nil},
		{name: "invalid array length", raw: "FFFFFFFE", err: errInvalidArrayLength},
		{name: "insufficient data", raw: "00000001", err: ErrInsufficientData},
		{name: "empty array", raw: "00000000", values: []int32{}},
		{name: "1 element", raw: "00000001 00000002", values: []int32{2}},
		{name: "many elements", raw: "00000003 00001267 FFFFFFFF 7FFFFFFF", values: []int32{4711, -1, 2147483647}},
	}
	for _, tc := range tt {
		a := assert.New(t)
		rd := realDecoder{
			raw: mustHexDecodeString(tc.raw),
		}
		values, err := rd.getInt32Array()
		if err != nil || tc.err != nil {
			a.Equal(tc.err, err)
		} else {
			a.Equal(tc.values, values)
		}
	}
}

func TestGetInt64Array(t *testing.T) {
	tt := []struct {
		name   string
		raw    string
		values []int64
		err    error
	}{
		{name: "nil array", raw: "FFFFFFFF", values: nil},
		{name: "invalid array length", raw: "FFFFFFFE", err: errInvalidArrayLength},
		{name: "insufficient data", raw: "00000001", err: ErrInsufficientData},
		{name: "empty array", raw: "00000000", values: []int64{}},
		{name: "1 element", raw: "00000001 0000000000000002", values: []int64{2}},
		{name: "many elements", raw: "00000002 FFFFFFFFFFFFFFFF 7FFFFFFFFFFFFFFF", values: []int64{-1, 9223372036854775807}},
	}
	for _, tc := range tt {
		a := assert.New(t)
		rd := realDecoder{
			raw: mustHexDecodeString(tc.raw),
		}
		values, err := rd.getInt64Array()
		if err != nil || tc.err != nil {
			a.Equal(tc.err, err)
		} else {
			a.Equal(tc.values, values)
		}
	}
}

func mustHexDecodeString(s string) []byte {
	s = strings.ReplaceAll(s, " ", "")
	raw, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return raw
}
