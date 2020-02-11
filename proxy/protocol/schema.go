package protocol

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"reflect"
)

var (
	typeBool               = &Bool{}
	typeInt16              = &Int16{}
	typeInt32              = &Int32{}
	typeStr                = &Str{}
	typeNullableStr        = &NullableStr{}
	typeCompactStr         = &CompactStr{}
	typeCompactNullableStr = &CompactNullableStr{}
)

type EncoderDecoder interface {
	decode(pd packetDecoder) (interface{}, error)     // reads / gets
	encode(pe packetEncoder, value interface{}) error // writes / puts
}

type Field interface {
	EncoderDecoder
	GetName() string
}

type Schema interface {
	EncoderDecoder
}

type schema struct {
	name         string
	fields       []boundField
	fieldsByName map[string]*boundField
}

//
type field struct {
	name string
	ty   EncoderDecoder
}

func (f *field) decode(pd packetDecoder) (interface{}, error) {
	return f.ty.decode(pd)
}

func (f *field) encode(pe packetEncoder, value interface{}) error {
	return f.ty.encode(pe, value)
}
func (f *field) GetName() string {
	return f.name
}

// bound field
type boundField struct {
	def    Field
	index  int
	schema *schema
}

// Field bool

type Bool struct{}

func (f *Bool) decode(pd packetDecoder) (interface{}, error) {
	return pd.getBool()
}

func (f *Bool) encode(pe packetEncoder, value interface{}) error {
	in, ok := value.(bool)
	if !ok {
		return SchemaEncodingError{fmt.Sprintf("value %T not a bool", value)}
	}
	pe.putBool(in)
	return nil
}

// Field int16

type Int16 struct{}

func (f *Int16) decode(pd packetDecoder) (interface{}, error) {
	return pd.getInt16()
}
func (f *Int16) encode(pe packetEncoder, value interface{}) error {
	in, ok := value.(int16)
	if !ok {
		return SchemaEncodingError{fmt.Sprintf("value %T not a int16", value)}
	}
	pe.putInt16(in)
	return nil
}

// Field int32

type Int32 struct{}

func (f *Int32) decode(pd packetDecoder) (interface{}, error) {
	return pd.getInt32()
}

func (f *Int32) encode(pe packetEncoder, value interface{}) error {
	in, ok := value.(int32)
	if !ok {
		return SchemaEncodingError{fmt.Sprintf("value %T not a int32", value)}
	}
	pe.putInt32(in)
	return nil
}

// Field string

type Str struct {
}

func (f *Str) decode(pd packetDecoder) (interface{}, error) {
	return pd.getString()
}

func (f *Str) encode(pe packetEncoder, value interface{}) error {
	in, ok := value.(string)
	if !ok {
		return SchemaEncodingError{fmt.Sprintf("value %T not a string", value)}
	}
	return pe.putString(in)
}

// Field nullable string

type NullableStr struct{}

func (f *NullableStr) decode(pd packetDecoder) (interface{}, error) {
	return pd.getNullableString()
}

func (f *NullableStr) encode(pe packetEncoder, value interface{}) error {
	if value == nil {
		if err := pe.putNullableString(nil); err != nil {
			return err
		}
	}

	in, ok := value.(*string)
	if !ok {
		return SchemaEncodingError{fmt.Sprintf("value %T not a *string", value)}
	}
	return pe.putNullableString(in)
}

// Field compact string

type CompactStr struct {
}

func (f *CompactStr) decode(pd packetDecoder) (interface{}, error) {
	return pd.getCompactString()
}

func (f *CompactStr) encode(pe packetEncoder, value interface{}) error {
	in, ok := value.(string)
	if !ok {
		return SchemaEncodingError{fmt.Sprintf("value %T not a string", value)}
	}
	return pe.putCompactString(in)
}

// Field compact nullable string

type CompactNullableStr struct{}

func (f *CompactNullableStr) decode(pd packetDecoder) (interface{}, error) {
	return pd.getCompactNullableString()
}

func (f *CompactNullableStr) encode(pe packetEncoder, value interface{}) error {
	if value == nil {
		if err := pe.putCompactNullableString(nil); err != nil {
			return err
		}
	}

	in, ok := value.(*string)
	if !ok {
		return SchemaEncodingError{fmt.Sprintf("value %T not a *string", value)}
	}
	return pe.putCompactNullableString(in)
}

// Arrays helper

func encodeArrayElements(in []interface{}, elementEncode func(pe packetEncoder, value interface{}) error, pe packetEncoder) (err error) {
	for _, elem := range in {
		err = elementEncode(pe, elem)
		if err != nil {
			return err
		}
	}
	return nil
}

func decodeArrayElements(n int, elementDecode func(pd packetDecoder) (interface{}, error), pd packetDecoder) (interface{}, error) {
	// We could allocate the capacity at once, but in case of malformed payload we could allocate too much memory.
	result := make([]interface{}, 0)

	for i := 0; i < n; i++ {
		elem, err := elementDecode(pd)
		if err != nil {
			return nil, err
		}
		result = append(result, elem)
	}
	return result, nil
}

// Tagged fields

type rawTaggedField struct {
	tag  int64
	data []byte
}

type taggedFields struct {
	name string
}

func (f *taggedFields) decode(pd packetDecoder) (interface{}, error) {
	numTaggedFields, err := pd.getVarint()
	if err != nil {
		return nil, err
	}
	if numTaggedFields == 0 {
		result := make([]rawTaggedField, 0)
		return result, nil
	}
	if numTaggedFields < 0 {
		return nil, errors.Errorf("Negative number of tagged fields %d", numTaggedFields)
	}
	result := make([]rawTaggedField, numTaggedFields)
	for i := 0; i < int(numTaggedFields); i++ {
		result[i].tag, err = pd.getVarint()
		if err != nil {
			return nil, err
		}
		result[i].data, err = pd.getVarintBytes()
		if err != nil {
			return nil, err
		}

	}
	return result, nil
}

func (f *taggedFields) encode(pe packetEncoder, value interface{}) error {
	in, ok := value.([]rawTaggedField)
	if !ok {
		return SchemaEncodingError{fmt.Sprintf("value %T not a []rawTaggedField", value)}
	}
	pe.putVarint(int64(len(in)))
	for _, rawTaggedField := range in {
		pe.putVarint(rawTaggedField.tag)
		err := pe.putVarintBytes(rawTaggedField.data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *taggedFields) GetName() string {
	return f.name
}

// Array

type array struct {
	name string
	ty   EncoderDecoder
}

func (f *array) decode(pd packetDecoder) (interface{}, error) {
	n, err := pd.getArrayLength()
	if err != nil {
		return nil, err
	}
	return decodeArrayElements(n, f.ty.decode, pd)
}

func (f *array) encode(pe packetEncoder, value interface{}) error {
	in, ok := value.([]interface{})
	if !ok {
		return SchemaEncodingError{fmt.Sprintf("value %T not a []interface{}", value)}
	}
	err := pe.putArrayLength(len(in))
	if err != nil {
		return err
	}
	return encodeArrayElements(in, f.ty.encode, pe)
}

func (f *array) GetName() string {
	return f.name
}

// Compact Array

type compactArray struct {
	name string
	ty   EncoderDecoder
}

func (f *compactArray) decode(pd packetDecoder) (interface{}, error) {
	n, err := pd.getCompactArrayLength()
	if err != nil {
		return nil, errors.Wrapf(err, "getCompactArrayLength field %s", f.name)
	}
	result, err := decodeArrayElements(n, f.ty.decode, pd)
	if err != nil {
		return nil, errors.Wrapf(err, "decodeArrayElements field %s", f.name)
	}
	return result, err
}

func (f *compactArray) encode(pe packetEncoder, value interface{}) error {
	in, ok := value.([]interface{})
	if !ok {
		return SchemaEncodingError{fmt.Sprintf("value %T not a []interface{}", value)}
	}
	err := pe.putCompactArrayLength(len(in))
	if err != nil {
		return err
	}
	return encodeArrayElements(in, f.ty.encode, pe)
}

func (f *compactArray) GetName() string {
	return f.name
}

// Compact nullable Array

type compactNullableArray struct {
	name string
	ty   EncoderDecoder
}

func (f *compactNullableArray) decode(pd packetDecoder) (interface{}, error) {
	n, err := pd.getCompactNullableArrayLength()
	if err != nil {
		return nil, err
	}
	if n == -1 {
		return nil, nil
	}
	return decodeArrayElements(n, f.ty.decode, pd)
}

func (f *compactNullableArray) encode(pe packetEncoder, value interface{}) error {
	if value == nil {
		return pe.putCompactNullableArrayLength(-1)
	}
	in, ok := value.([]interface{})
	if !ok {
		return SchemaEncodingError{fmt.Sprintf("value %T not a []interface{}", value)}
	}

	err := pe.putCompactNullableArrayLength(len(in))
	if err != nil {
		return err
	}
	return encodeArrayElements(in, f.ty.encode, pe)
}

func (f *compactNullableArray) GetName() string {
	return f.name
}

type Struct struct {
	schema *schema
	values []interface{}
}

func (s Struct) String() string {
	var buffer bytes.Buffer
	buffer.WriteString(s.schema.name + "{")
	for i, field := range s.schema.fields {
		if i != 0 {
			buffer.WriteString(",")
		}
		name := field.def.GetName()
		buffer.WriteString(fmt.Sprintf("%s:%v", name, s.Get(name)))
	}
	buffer.WriteString("}")
	return buffer.String()
}

func (s Struct) Get(name string) interface{} {
	bf := s.schema.fieldsByName[name]
	if bf == nil || bf.index >= len(s.values) {
		return nil
	}
	return s.values[bf.index]
}

func (s *Struct) Replace(name string, value interface{}) error {
	if value == nil {
		return fmt.Errorf("field %s value in struct %s : new value must not be nil", name, s.schema.name)
	}
	bf := s.schema.fieldsByName[name]
	if bf == nil {
		return fmt.Errorf("field %s value in struct %s : name not found", name, s.schema.name)
	}
	if bf.index >= len(s.values) {
		return fmt.Errorf("field %s value in struct %s : index %d gte %d", name, s.schema.name, bf.index, len(s.values))
	}
	v := s.values[bf.index]
	if v == nil {
		return fmt.Errorf("field %s value in struct %s : old value not found", name, s.schema.name)
	}
	oldKind := reflect.TypeOf(v).Kind()
	newKind := reflect.TypeOf(value).Kind()
	if oldKind != newKind {
		return fmt.Errorf("field %s value in struct %s : kinds differ %v to %v", name, s.schema.name, oldKind, newKind)
	}
	s.values[bf.index] = value
	return nil
}

// NewSchema creates new schema. It panics when a duplicate field is provided
func NewSchema(name string, fs ...Field) Schema {

	s := &schema{name: name, fields: make([]boundField, 0), fieldsByName: make(map[string]*boundField)}

	for i, f := range fs {
		if _, ok := s.fieldsByName[f.GetName()]; ok {
			panic(fmt.Sprintf("Schema contains a duplicate field: %s", f.GetName()))
		}
		bf := boundField{
			def:    f,
			index:  i,
			schema: s,
		}
		s.fields = append(s.fields, bf)
		s.fieldsByName[f.GetName()] = &bf
	}
	return s
}

func (s *schema) encode(pe packetEncoder, value interface{}) error {
	in, ok := value.(*Struct)
	if !ok {
		return SchemaEncodingError{fmt.Sprintf("value %T not a *Struct", value)}
	}
	if len(in.values) != len(s.fields) {
		return SchemaEncodingError{fmt.Sprintf("length difference: values %d, struct fields %d", len(in.values), len(s.fields))}
	}
	for i, value := range in.values {
		if err := s.fields[i].def.encode(pe, value); err != nil {
			return err
		}
	}
	return nil
}

func (s *schema) decode(pd packetDecoder) (interface{}, error) {
	values := make([]interface{}, 0)

	for _, field := range s.fields {
		value, err := field.def.decode(pd)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return &Struct{schema: s, values: values}, nil
}

func DecodeSchema(buf []byte, schema Schema) (*Struct, error) {
	if buf == nil {
		return nil, nil
	}
	helper := realDecoder{raw: buf}
	v, err := schema.decode(&helper)
	if err != nil {
		return nil, err
	}
	if helper.off != len(buf) {
		return nil, SchemaDecodingError{"invalid length"}
	}

	st, ok := v.(*Struct)
	if !ok {
		return nil, SchemaDecodingError{"internal error: schema decode should return *Struct"}
	}
	//log.Printf("Decoded Schema %v", v)

	return st, nil
}

func EncodeSchema(s *Struct, schema Schema) ([]byte, error) {
	if s == nil {
		return nil, nil
	}

	var prepEnc prepEncoder
	var realEnc realEncoder

	err := schema.encode(&prepEnc, s)
	if err != nil {
		return nil, err
	}

	if prepEnc.length < 0 || prepEnc.length > int(MaxRequestSize) {
		return nil, SchemaEncodingError{fmt.Sprintf("invalid request size (%d)", prepEnc.length)}
	}

	realEnc.raw = make([]byte, prepEnc.length)
	err = schema.encode(&realEnc, s)
	if err != nil {
		return nil, err
	}

	return realEnc.raw, nil
}
