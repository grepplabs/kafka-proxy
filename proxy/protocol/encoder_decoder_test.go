package protocol

import (
	"bytes"
	"math"
	"math/rand"
	"testing"

	"github.com/pkg/errors"
)

func TestEncodeDecodeCompactBytes(t *testing.T) {
	tt := []struct {
		name string
		lens []int
	}{
		{name: "empty string", lens: []int{0}},
		{name: "empty strings", lens: []int{0, 0}},
		{name: "len 1", lens: []int{1}},
		{name: "lens 1", lens: []int{1, 1}},
		{name: "len 2", lens: []int{2}},
		{name: "len 3", lens: []int{3}},
		{name: "len 4", lens: []int{4}},
		{name: "len 16", lens: []int{16}},
		{name: "len 63", lens: []int{63}},
		{name: "len 64", lens: []int{64}},
		{name: "len 128", lens: []int{128}},
		{name: "len 8191", lens: []int{8191}},
		{name: "len 8192", lens: []int{8192}},
		{name: "len 32767", lens: []int{math.MaxInt16}},
		{name: "lens 32767", lens: []int{math.MaxInt16, math.MaxInt16}},
		{name: "different values", lens: []int{0, 1, 2, 3, 4, 16, 64, 127, 128, 8191, 8192, 8191, 128, 127, 64, 16, 4, 3, 2, 1, 0}},
	}
	for _, tc := range tt {
		values := make([][]byte, 0)
		for _, l := range tc.lens {
			value := RandStringRunes(l)
			values = append(values, []byte(value))
		}
		request := &CompactBytesHolder{
			values: values,
		}
		buf, err := Encode(request)
		if err != nil {
			t.Fatal(err)
		}
		response := &CompactBytesHolder{}
		err = Decode(buf, response)
		if err != nil {
			t.Fatal(err)
		}
		if len(request.values) != len(response.values) {
			t.Fatalf("Values array lengths differ: expected %v, actual %v", request.values, response.values)
		}
		for i := range request.values {
			if bytes.Compare(request.values[i], response.values[i]) != 0 {
				t.Fatalf("Values differ: index %d, expected %v, actual %v", i, request.values[i], response.values[i])
			}
		}
	}
}

func TestEncodeDecodeCompactString(t *testing.T) {
	tt := []struct {
		name string
		lens []int
	}{
		{name: "empty string", lens: []int{0}},
		{name: "empty strings", lens: []int{0, 0}},
		{name: "len 1", lens: []int{1}},
		{name: "lens 1", lens: []int{1, 1}},
		{name: "len 2", lens: []int{2}},
		{name: "len 3", lens: []int{3}},
		{name: "len 4", lens: []int{4}},
		{name: "len 16", lens: []int{16}},
		{name: "len 63", lens: []int{63}},
		{name: "len 64", lens: []int{64}},
		{name: "len 128", lens: []int{128}},
		{name: "len 8191", lens: []int{8191}},
		{name: "len 8192", lens: []int{8192}},
		{name: "len 32767", lens: []int{math.MaxInt16}},
		{name: "lens 32767", lens: []int{math.MaxInt16, math.MaxInt16}},
		{name: "different values", lens: []int{0, 1, 2, 3, 4, 16, 64, 127, 128, 8191, 8192, 8191, 128, 127, 64, 16, 4, 3, 2, 1, 0}},
	}
	for _, tc := range tt {
		values := make([]string, 0)
		for _, l := range tc.lens {
			value := RandStringRunes(l)
			values = append(values, value)
		}
		request := &CompactStringsHolder{
			values: values,
		}
		buf, err := Encode(request)
		if err != nil {
			t.Fatal(err)
		}
		response := &CompactStringsHolder{}
		err = Decode(buf, response)
		if err != nil {
			t.Fatal(err)
		}
		if len(request.values) != len(response.values) {
			t.Fatalf("Values array lengths differ: expected %v, actual %v", request.values, response.values)
		}
		for i := range request.values {
			if request.values[i] != response.values[i] {
				t.Fatalf("Values differ: index %d, expected %v, actual %v", i, request.values[i], response.values[i])
			}
		}
	}
}

func TestEncodeDecodeCompactNullableString(t *testing.T) {
	tt := []struct {
		name string
		lens []int
	}{
		{name: "nil ref", lens: []int{-1}},
		{name: "nil refs", lens: []int{-1, -1}},
		{name: "empty string", lens: []int{0}},
		{name: "empty strings", lens: []int{0, 0}},
		{name: "len 1", lens: []int{1}},
		{name: "lens 1", lens: []int{1, 1}},
		{name: "len 2", lens: []int{2}},
		{name: "len 3", lens: []int{3}},
		{name: "len 4", lens: []int{4}},
		{name: "len 16", lens: []int{16}},
		{name: "len 63", lens: []int{63}},
		{name: "len 64", lens: []int{64}},
		{name: "len 128", lens: []int{128}},
		{name: "len 8191", lens: []int{8191}},
		{name: "len 8192", lens: []int{8192}},
		{name: "len 32767", lens: []int{math.MaxInt16}},
		{name: "lens 32767", lens: []int{math.MaxInt16, math.MaxInt16}},
		{name: "different values", lens: []int{0, 1, 2, 3, 4, -1, 16, 64, 127, 128, 8191, 8192, 8191, 128, 127, -1, 64, 16, 4, 3, 2, 1, -1, 0}},
	}
	for _, tc := range tt {
		values := make([]*string, 0)
		for _, l := range tc.lens {
			if l >= 0 {
				value := RandStringRunes(l)
				values = append(values, &value)
			} else {
				values = append(values, nil)
			}
		}
		request := &CompactNullableStringsHolder{
			values: values,
		}
		buf, err := Encode(request)
		if err != nil {
			t.Fatal(err)
		}
		response := &CompactNullableStringsHolder{}
		err = Decode(buf, response)
		if err != nil {
			t.Fatal(err)
		}
		if len(request.values) != len(response.values) {
			t.Fatalf("Values array lengths differ: expected %v, actual %v", request.values, response.values)
		}
		for i := range request.values {
			if (request.values[i] != nil) != (response.values[i] != nil) {
				t.Errorf("is nil comperison differ: expected %v, actual %v", request.values[i] != nil, response.values[i] != nil)
			}
			if request.values[i] != nil && *request.values[i] != *response.values[i] {
				t.Fatalf("Values differ: index %d, expected %v, actual %v", i, request.values[i], response.values[i])
			}
		}
	}
}

func TestEncodeDecodeCompactArray(t *testing.T) {
	compactArray := &compactArray{name: "strings", ty: typeStr}
	tt := []struct {
		name string
		lens []int
	}{
		{name: "empty string", lens: []int{0}},
		{name: "empty strings", lens: []int{0, 0}},
		{name: "len 1", lens: []int{1}},
		{name: "lens 1", lens: []int{1, 1}},
		{name: "len 2", lens: []int{2}},
		{name: "len 3", lens: []int{3}},
		{name: "len 4", lens: []int{4}},
		{name: "len 16", lens: []int{16}},
		{name: "len 63", lens: []int{63}},
		{name: "len 64", lens: []int{64}},
		{name: "len 128", lens: []int{128}},
		{name: "len 8191", lens: []int{8191}},
		{name: "len 8192", lens: []int{8192}},
		{name: "len 32767", lens: []int{math.MaxInt16}},
		{name: "lens 32767", lens: []int{math.MaxInt16, math.MaxInt16}},
		{name: "different values", lens: []int{0, 1, 2, 3, 4, 16, 64, 127, 128, 8191, 8192, 8191, 128, 127, 64, 16, 4, 3, 2, 1, 0}},
	}
	for _, tc := range tt {
		values := make([]interface{}, 0)
		for _, l := range tc.lens {
			value := RandStringRunes(l)
			values = append(values, value)
		}
		request := &CompactArrayHolder{
			values: values,
			array:  compactArray,
		}
		buf, err := Encode(request)
		if err != nil {
			t.Fatal(err)
		}
		response := &CompactArrayHolder{
			array: compactArray,
		}
		err = Decode(buf, response)
		if err != nil {
			t.Fatal(err)
		}
		if len(request.values) != len(response.values) {
			t.Fatalf("Values array lengths differ: expected %v, actual %v", request.values, response.values)
		}
		for i := range request.values {
			if (request.values[i] != nil) != (response.values[i] != nil) {
				t.Errorf("is nil comperison differ: expected %v, actual %v", request.values[i] != nil, response.values[i] != nil)
			}
			if request.values[i] != nil && request.values[i] != response.values[i] {
				t.Fatalf("Values differ: index %d, expected %v, actual %v", i, request.values[i], response.values[i])
			}
		}
	}
}

func TestEncodeDecodeCompactNullableArray(t *testing.T) {
	array := &compactNullableArray{name: "strings", ty: typeNullableStr}
	tt := []struct {
		name string
		lens []int
	}{
		{name: "nil ref", lens: []int{-1}},
		{name: "nil refs", lens: []int{-1, -1}},
		{name: "empty string", lens: []int{0}},
		{name: "empty strings", lens: []int{0, 0}},
		{name: "len 1", lens: []int{1}},
		{name: "lens 1", lens: []int{1, 1}},
		{name: "len 2", lens: []int{2}},
		{name: "len 3", lens: []int{3}},
		{name: "len 4", lens: []int{4}},
		{name: "len 16", lens: []int{16}},
		{name: "len 63", lens: []int{63}},
		{name: "len 64", lens: []int{64}},
		{name: "len 128", lens: []int{128}},
		{name: "len 8191", lens: []int{8191}},
		{name: "len 8192", lens: []int{8192}},
		{name: "len 32767", lens: []int{math.MaxInt16}},
		{name: "lens 32767", lens: []int{math.MaxInt16, math.MaxInt16}},
		{name: "different values", lens: []int{0, 1, 2, 3, 4, -1, 16, 64, 127, 128, 8191, 8192, 8191, 128, 127, -1, 64, 16, 4, 3, 2, 1, -1, 0}},
	}
	for _, tc := range tt {
		values := make([]interface{}, 0)
		for _, l := range tc.lens {
			if l >= 0 {
				value := RandStringRunes(l)
				values = append(values, &value)
			} else {
				values = append(values, new(string))
			}
		}
		request := &CompactNullableArrayHolder{
			values: values,
			array:  array,
		}
		buf, err := Encode(request)
		if err != nil {
			t.Fatal(err)
		}
		response := &CompactNullableArrayHolder{
			array: array,
		}
		err = Decode(buf, response)
		if err != nil {
			t.Fatal(err)
		}
		if len(request.values) != len(response.values) {
			t.Fatalf("Values array lengths differ: expected %v, actual %v", request.values, response.values)
		}
		for i := range request.values {
			if (request.values[i].(*string) != nil) != (response.values[i].(*string) != nil) {
				t.Errorf("is nil comperison differ: expected %v, actual %v", request.values[i] != nil, response.values[i] != nil)
			}
			if request.values[i].(*string) != nil && *request.values[i].(*string) != *response.values[i].(*string) {
				t.Fatalf("Values differ: index %d, expected %v, actual %v", i, request.values[i].(*string), response.values[i].(*string))
			}
		}
	}
}

func TestEncodeDecodeInt32Array(t *testing.T) {
	tt := []struct {
		name   string
		values []int32
	}{
		{name: "nil array", values: nil},
		{name: "empty array", values: []int32{}},
		{name: "0", values: []int32{0}},
		{name: "0s", values: []int32{0, 0}},
		{name: "1", values: []int32{1}},
		{name: "1s", values: []int32{1, 1}},
		{name: "2", values: []int32{2}},
		{name: "3", values: []int32{3}},
		{name: "4", values: []int32{4}},
		{name: "16", values: []int32{16}},
		{name: "63", values: []int32{63}},
		{name: "64", values: []int32{64}},
		{name: "128", values: []int32{128}},
		{name: "8191", values: []int32{8191}},
		{name: "8192", values: []int32{8192}},
		{name: "32767", values: []int32{math.MaxInt16}},
		{name: "32767,-32768", values: []int32{math.MaxInt16, math.MinInt16}},
		{name: "2147483647,-2147483648", values: []int32{math.MaxInt32, math.MinInt32}},
		{name: "different values", values: []int32{0, 1, 2, 3, 4, -1, 16, 64, 127, 128, 8191, 8192, 8191, 128, 127, -1, 64, 16, 4, 3, 2, 1, -1, 0}},
	}
	for _, tc := range tt {
		request := &Int32ArrayHolder{
			values: tc.values,
		}
		buf, err := Encode(request)
		if err != nil {
			t.Fatal(err)
		}
		response := &Int32ArrayHolder{}
		err = Decode(buf, response)
		if err != nil {
			t.Fatal(err)
		}
		if (request.values == nil && response.values != nil) || (request.values != nil && response.values == nil) {
			t.Fatalf("Nils comparison error: expected %v, actual %v", request.values == nil, response.values == nil)
		}
		if len(request.values) != len(response.values) {
			t.Fatalf("Values array lengths differ: expected %v, actual %v", request.values, response.values)
		}
		for i := range request.values {
			if request.values[i] != response.values[i] {
				t.Fatalf("Values differ: index %d, expected %v, actual %v", i, request.values[i], response.values[i])
			}
		}
	}
}

func TestEncodeDecodeInt64Array(t *testing.T) {
	tt := []struct {
		name   string
		values []int64
	}{
		{name: "nil array", values: nil},
		{name: "empty array", values: []int64{}},
		{name: "0", values: []int64{0}},
		{name: "0s", values: []int64{0, 0}},
		{name: "1", values: []int64{1}},
		{name: "1s", values: []int64{1, 1}},
		{name: "2", values: []int64{2}},
		{name: "3", values: []int64{3}},
		{name: "4", values: []int64{4}},
		{name: "16", values: []int64{16}},
		{name: "63", values: []int64{63}},
		{name: "64", values: []int64{64}},
		{name: "128", values: []int64{128}},
		{name: "8191", values: []int64{8191}},
		{name: "8192", values: []int64{8192}},
		{name: "32767", values: []int64{math.MaxInt16}},
		{name: "32767,-32768", values: []int64{math.MaxInt16, math.MinInt16}},
		{name: "2147483647", values: []int64{math.MaxInt32}},
		{name: "2147483647,-2147483648", values: []int64{math.MaxInt32, math.MinInt32}},
		{name: "max,min", values: []int64{math.MaxInt64, math.MinInt64}},
		{name: "different values", values: []int64{0, 1, 2, 3, 4, -1, 16, 64, 127, 128, 8191, 8192, 8191, 128, 127, -1, 64, 16, 4, 3, 2, 1, -1, 0}},
	}
	for _, tc := range tt {
		request := &Int64ArrayHolder{
			values: tc.values,
		}
		buf, err := Encode(request)
		if err != nil {
			t.Fatal(err)
		}
		response := &Int64ArrayHolder{}
		err = Decode(buf, response)
		if err != nil {
			t.Fatal(err)
		}
		if (request.values == nil && response.values != nil) || (request.values != nil && response.values == nil) {
			t.Fatalf("%s: Nils comparison error: expected %v, actual %v", tc.name, request.values == nil, response.values == nil)
		}
		if len(request.values) != len(response.values) {
			t.Fatalf("Values array lengths differ: expected %v, actual %v", request.values, response.values)
		}
		for i := range request.values {
			if request.values[i] != response.values[i] {
				t.Fatalf("Values differ: index %d, expected %v, actual %v", i, request.values[i], response.values[i])
			}
		}
	}
}

type Int32ArrayHolder struct {
	values []int32
}

func (r *Int32ArrayHolder) encode(pe packetEncoder) (err error) {
	err = pe.putInt32Array(r.values)
	if err != nil {
		return err
	}
	return
}
func (r *Int32ArrayHolder) decode(pd packetDecoder) (err error) {
	if r.values, err = pd.getInt32Array(); err != nil {
		return err
	}
	if pd.remaining() != 0 {
		return errors.Errorf("remaining bytes %d", pd.remaining())
	}
	return
}

type Int64ArrayHolder struct {
	values []int64
}

func (r *Int64ArrayHolder) encode(pe packetEncoder) (err error) {
	err = pe.putInt64Array(r.values)
	if err != nil {
		return err
	}
	return
}
func (r *Int64ArrayHolder) decode(pd packetDecoder) (err error) {
	if r.values, err = pd.getInt64Array(); err != nil {
		return err
	}
	if pd.remaining() != 0 {
		return errors.Errorf("remaining bytes %d", pd.remaining())
	}
	return
}

type CompactBytesHolder struct {
	values [][]byte
}

func (r *CompactBytesHolder) encode(pe packetEncoder) (err error) {
	for _, value := range r.values {
		err = pe.putCompactBytes(value)
		if err != nil {
			return err
		}
	}
	return
}

func (r *CompactBytesHolder) decode(pd packetDecoder) (err error) {
	r.values = make([][]byte, 0)
	var value []byte
	for ok := true; ok; ok = pd.remaining() > 0 {
		if value, err = pd.getCompactBytes(); err != nil {
			return err
		}
		r.values = append(r.values, value)
	}
	if pd.remaining() != 0 {
		return errors.Errorf("remaining bytes %d", pd.remaining())
	}
	return
}

type CompactStringsHolder struct {
	values []string
}

func (r *CompactStringsHolder) encode(pe packetEncoder) (err error) {
	for _, value := range r.values {
		err = pe.putCompactString(value)
		if err != nil {
			return err
		}
	}
	return
}

func (r *CompactStringsHolder) decode(pd packetDecoder) (err error) {
	r.values = make([]string, 0)
	var value string
	for ok := true; ok; ok = pd.remaining() > 0 {
		if value, err = pd.getCompactString(); err != nil {
			return err
		}
		r.values = append(r.values, value)
	}
	if pd.remaining() != 0 {
		return errors.Errorf("remaining bytes %d", pd.remaining())
	}
	return
}

type CompactNullableStringsHolder struct {
	values []*string
}

func (r *CompactNullableStringsHolder) encode(pe packetEncoder) (err error) {
	for _, value := range r.values {
		err = pe.putCompactNullableString(value)
		if err != nil {
			return err
		}
	}
	return
}

func (r *CompactNullableStringsHolder) decode(pd packetDecoder) (err error) {
	r.values = make([]*string, 0)
	var value *string
	for ok := true; ok; ok = pd.remaining() > 0 {
		if value, err = pd.getCompactNullableString(); err != nil {
			return err
		}
		r.values = append(r.values, value)
	}
	if pd.remaining() != 0 {
		return errors.Errorf("remaining bytes %d", pd.remaining())
	}
	return
}

type CompactArrayHolder struct {
	values []interface{}
	array  *compactArray
}

func (r *CompactArrayHolder) encode(pe packetEncoder) (err error) {
	return r.array.encode(pe, r.values)
}

func (r *CompactArrayHolder) decode(pd packetDecoder) (err error) {
	vs, err := r.array.decode(pd)
	if err != nil {
		return err
	}
	in, ok := vs.([]interface{})
	if !ok {
		return errors.New("decoded value not a []interface{}")
	}
	r.values = in

	if pd.remaining() != 0 {
		return errors.Errorf("remaining bytes %d", pd.remaining())
	}
	return
}

type CompactNullableArrayHolder struct {
	values []interface{}
	array  *compactNullableArray
}

func (r *CompactNullableArrayHolder) encode(pe packetEncoder) (err error) {
	return r.array.encode(pe, r.values)
}

func (r *CompactNullableArrayHolder) decode(pd packetDecoder) (err error) {
	vs, err := r.array.decode(pd)
	if err != nil {
		return err
	}
	in, ok := vs.([]interface{})
	if !ok {
		return errors.New("decoded value not a []interface{}")
	}
	r.values = in

	if pd.remaining() != 0 {
		return errors.Errorf("remaining bytes %d", pd.remaining())
	}
	return
}

func RandStringRunes(n int) string {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
