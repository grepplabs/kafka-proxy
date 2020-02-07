package protocol

import (
	"github.com/pkg/errors"
	"math"
	"math/rand"
	"testing"
)

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
