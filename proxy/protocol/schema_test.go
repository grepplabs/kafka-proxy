package protocol

import "testing"

func TestEncodeDecodeSchema(t *testing.T) {
	someSchema := NewSchema("some_schema",
		&Mfield{Name: "fieldInt16", Ty: TypeInt16},
	)

	schema := NewSchema("test_schema",
		&Mfield{Name: "fieldInt16", Ty: TypeInt16},
		&Mfield{Name: "fieldInt32", Ty: TypeInt32},
		&Mfield{Name: "fieldBool", Ty: TypeBool},
		&Mfield{Name: "fieldStr", Ty: TypeStr},
		&Mfield{Name: "fieldNullableStr", Ty: TypeNullableStr},
		&Mfield{Name: "fieldCompactStr", Ty: TypeCompactStr},
		&Mfield{Name: "fieldCompactNullableStr", Ty: TypeCompactNullableStr},
		&Array{Name: "arrayOfSomeSchema", Ty: someSchema},
	)

	someData := &Struct{
		Schema: someSchema,
		Values: make([]interface{}, 0),
	}

	data := &Struct{
		Schema: schema,
		Values: make([]interface{}, 0),
	}

	someData.Values = append(someData.Values, int16(20))

	nStr := "nullableStr"
	nCompactStr := "nullableCompactStr"
	data.Values = append(data.Values, int16(10))
	data.Values = append(data.Values, int32(10))
	data.Values = append(data.Values, false)
	data.Values = append(data.Values, "str")
	data.Values = append(data.Values, &nStr)
	data.Values = append(data.Values, "compactStr")
	data.Values = append(data.Values, &nCompactStr)
	someArr := make([]interface{}, 0)
	someArr = append(someArr, someData)
	data.Values = append(data.Values, someArr)

	result, err := EncodeSchema(data, schema)

	if err != nil {
		t.Fatalf("Encoding schema failed %s", err)
	}

	resultData, err := DecodeSchema(result, schema)

	if err != nil {
		t.Fatalf("Decoding schema failed %s", err)
	}

	val := resultData.Get("fieldInt16")

	if val != int16(10) {
		t.Fatalf("Bad value of decoded input, expected 10, got %d", val)
	}

	fieldInt16Schema := schema.GetFieldsByName()["fieldInt16"].GetDef().GetSchema()

	if fieldInt16Schema != TypeInt16 {
		t.Fatalf("Got bad schema for TypeInt16 field")
	}

	val = resultData.Get("fieldInt32")

	if val != int32(10) {
		t.Fatalf("Bad value of decoded input, expected 10, got %d", val)
	}

	fieldInt32Schema := schema.GetFieldsByName()["fieldInt32"].GetDef().GetSchema()

	if fieldInt32Schema != TypeInt32 {
		t.Fatalf("Got bad schema for TypeInt32 field")
	}

	val = resultData.Get("fieldBool")

	if val != false {
		t.Fatalf("Bad value of decoded input, expected false, got %b", val)
	}

	fieldBoolSchema := schema.GetFieldsByName()["fieldBool"].GetDef().GetSchema()

	if fieldBoolSchema != TypeBool {
		t.Fatalf("Got bad schema for TypeBool field")
	}

	val = resultData.Get("fieldStr")

	if val != "str" {
		t.Fatalf("Bad value of decoded input, expected str, got %s", val)
	}

	fieldStrSchema := schema.GetFieldsByName()["fieldStr"].GetDef().GetSchema()

	if fieldStrSchema != TypeStr {
		t.Fatalf("Got bad schema for TypeStr field")
	}

	val = resultData.Get("fieldNullableStr")

	if *(val.(*string)) != "nullableStr" {
		t.Fatalf("Bad value of decoded input, expected nullableStr, got %s", val)
	}

	fieldNullStrSchema := schema.GetFieldsByName()["fieldNullableStr"].GetDef().GetSchema()

	if fieldNullStrSchema != TypeNullableStr {
		t.Fatalf("Got bad schema for TypeNullableStr field")
	}

	val = resultData.Get("fieldCompactStr")

	if val != "compactStr" {
		t.Fatalf("Bad value of decoded input, expected compactStr, got %s", val)
	}

	fieldCompactStrSchema := schema.GetFieldsByName()["fieldCompactStr"].GetDef().GetSchema()

	if fieldCompactStrSchema != TypeCompactStr {
		t.Fatalf("Got bad schema for fieldCompactStr field")
	}

	val = resultData.Get("fieldCompactNullableStr")

	if *(val.(*string)) != "nullableCompactStr" {
		t.Fatalf("Bad value of decoded input, expected nullableCompactStr, got %s", val)
	}

	fieldCompactNullableStrSchema := schema.GetFieldsByName()["fieldCompactNullableStr"].GetDef().GetSchema()

	if fieldCompactNullableStrSchema != TypeCompactNullableStr {
		t.Fatalf("Got bad schema for fieldCompactNullableStr field")
	}

	subSchema := schema.GetFieldsByName()["arrayOfSomeSchema"].GetDef().GetSchema()

	if subSchema != someSchema {
		t.Fatalf("Schema of child retrieved from parent does not match child schema declared")
	}

	val = resultData.Get("arrayOfSomeSchema")
	b := val.([]interface{})

	if b[0].(*Struct).Get("fieldInt16") != int16(20) {
		t.Fatalf("Child schema bad value, expected 20, got %d", b[0].(*Struct).Get("fieldInt16"))
	}

}
