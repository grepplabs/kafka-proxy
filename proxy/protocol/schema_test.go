package protocol

import "testing"

func TestEncodeDecodeSchema(t *testing.T) {
	someSchema := NewSchema("some_schema",
		&field{name: "fieldInt16", ty: typeInt16},
	)

	schema := NewSchema("test_schema",
		&field{name: "fieldInt16", ty: typeInt16},
		&field{name: "fieldInt32", ty: typeInt32},
		&field{name: "fieldBool", ty: typeBool},
		&field{name: "fieldStr", ty: typeStr},
		&field{name: "fieldNullableStr", ty: typeNullableStr},
		&field{name: "fieldCompactStr", ty: typeCompactStr},
		&field{name: "fieldCompactNullableStr", ty: typeCompactNullableStr},
		&array{name: "arrayOfSomeSchema", ty: someSchema},
	)

	someData := &Struct{
		schema: someSchema,
		values: make([]interface{}, 0),
	}

	data := &Struct{
		schema: schema,
		values: make([]interface{}, 0),
	}

	someData.values = append(someData.values, int16(20))

	nStr := "nullableStr"
	nCompactStr := "nullableCompactStr"
	data.values = append(data.values, int16(10))
	data.values = append(data.values, int32(10))
	data.values = append(data.values, false)
	data.values = append(data.values, "str")
	data.values = append(data.values, &nStr)
	data.values = append(data.values, "compactStr")
	data.values = append(data.values, &nCompactStr)
	someArr := make([]interface{}, 0)
	someArr = append(someArr, someData)
	data.values = append(data.values, someArr)

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

	if fieldInt16Schema != typeInt16 {
		t.Fatalf("Got bad schema for typeInt16 field")
	}

	val = resultData.Get("fieldInt32")

	if val != int32(10) {
		t.Fatalf("Bad value of decoded input, expected 10, got %d", val)
	}

	fieldInt32Schema := schema.GetFieldsByName()["fieldInt32"].GetDef().GetSchema()

	if fieldInt32Schema != typeInt32 {
		t.Fatalf("Got bad schema for typeInt32 field")
	}

	val = resultData.Get("fieldBool")

	if val != false {
		t.Fatalf("Bad value of decoded input, expected false, got %b", val)
	}

	fieldBoolSchema := schema.GetFieldsByName()["fieldBool"].GetDef().GetSchema()

	if fieldBoolSchema != typeBool {
		t.Fatalf("Got bad schema for typeBool field")
	}

	val = resultData.Get("fieldStr")

	if val != "str" {
		t.Fatalf("Bad value of decoded input, expected str, got %s", val)
	}

	fieldStrSchema := schema.GetFieldsByName()["fieldStr"].GetDef().GetSchema()

	if fieldStrSchema != typeStr {
		t.Fatalf("Got bad schema for typeStr field")
	}

	val = resultData.Get("fieldNullableStr")

	if *(val.(*string)) != "nullableStr" {
		t.Fatalf("Bad value of decoded input, expected nullableStr, got %s", val)
	}

	fieldNullStrSchema := schema.GetFieldsByName()["fieldNullableStr"].GetDef().GetSchema()

	if fieldNullStrSchema != typeNullableStr {
		t.Fatalf("Got bad schema for typeNullableStr field")
	}

	val = resultData.Get("fieldCompactStr")

	if val != "compactStr" {
		t.Fatalf("Bad value of decoded input, expected compactStr, got %s", val)
	}

	fieldCompactStrSchema := schema.GetFieldsByName()["fieldCompactStr"].GetDef().GetSchema()

	if fieldCompactStrSchema != typeCompactStr {
		t.Fatalf("Got bad schema for fieldCompactStr field")
	}

	val = resultData.Get("fieldCompactNullableStr")

	if *(val.(*string)) != "nullableCompactStr" {
		t.Fatalf("Bad value of decoded input, expected nullableCompactStr, got %s", val)
	}

	fieldCompactNullableStrSchema := schema.GetFieldsByName()["fieldCompactNullableStr"].GetDef().GetSchema()

	if fieldCompactNullableStrSchema != typeCompactNullableStr {
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
