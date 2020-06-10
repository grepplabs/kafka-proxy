package protocol

import (
	"testing"
)

func TestDeleteTopicsRequestV0(t *testing.T) {
	schemaDeleteTopics := NewSchemaStruct("delete_topics_v0",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topic", ty: typeStr},
		&field{name: "timeout_ms", ty: typeInt32},
	)

	topicMetadata := Struct{
		schema: schemaDeleteTopics,
		values: make([]interface{}, 0),
	}

	reqT := &DeleteTopicsRequestV0{}

	topicMetadata.values = append(topicMetadata.values, int16(reqT.key()))
	topicMetadata.values = append(topicMetadata.values, int16(reqT.version()))
	topicMetadata.values = append(topicMetadata.values, int32(0))
	topicMetadata.values = append(topicMetadata.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, "topic1")
	arr = append(arr, "topic2")
	topicMetadata.values = append(topicMetadata.values, arr)
	topicMetadata.values = append(topicMetadata.values, int32(0))

	encoded, err := EncodeSchema(&topicMetadata, schemaDeleteTopics)

	if err != nil {
		t.Errorf("Test failed, encoding schema %s", err)
	}

	req := &Request{Body: reqT}
	if err = Decode(encoded, req); err != nil {
		t.Errorf("Test failed decoding request, %s", err)
	}

	if len(reqT.GetTopics()) != len(arr) {
		t.Errorf("Number of topics after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
	}

	for i, val := range reqT.GetTopics() {
		if val != arr[i] {
			t.Errorf("Topics gathered after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
		}
	}
}

func TestDeleteTopicsRequestV1(t *testing.T) {
	schemaDeleteTopics := NewSchemaStruct("delete_topics_v1",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topic", ty: typeStr},
		&field{name: "timeout_ms", ty: typeInt32},
	)

	topicMetadata := Struct{
		schema: schemaDeleteTopics,
		values: make([]interface{}, 0),
	}

	reqT := &DeleteTopicsRequestV1{}

	topicMetadata.values = append(topicMetadata.values, int16(reqT.key()))
	topicMetadata.values = append(topicMetadata.values, int16(reqT.version()))
	topicMetadata.values = append(topicMetadata.values, int32(0))
	topicMetadata.values = append(topicMetadata.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, "topic1")
	arr = append(arr, "topic2")
	topicMetadata.values = append(topicMetadata.values, arr)
	topicMetadata.values = append(topicMetadata.values, int32(0))

	encoded, err := EncodeSchema(&topicMetadata, schemaDeleteTopics)

	if err != nil {
		t.Errorf("Test failed, encoding schema %s", err)
	}

	req := &Request{Body: reqT}
	if err = Decode(encoded, req); err != nil {
		t.Errorf("Test failed decoding request, %s", err)
	}

	if len(reqT.GetTopics()) != len(arr) {
		t.Errorf("Number of topics after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
	}

	for i, val := range reqT.GetTopics() {
		if val != arr[i] {
			t.Errorf("Topics gathered after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
		}
	}
}

func TestDeleteTopicsRequestV2(t *testing.T) {
	schemaDeleteTopics := NewSchemaStruct("delete_topics_v2",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topic", ty: typeStr},
		&field{name: "timeout_ms", ty: typeInt32},
	)

	topicMetadata := Struct{
		schema: schemaDeleteTopics,
		values: make([]interface{}, 0),
	}

	reqT := &DeleteTopicsRequestV2{}

	topicMetadata.values = append(topicMetadata.values, int16(reqT.key()))
	topicMetadata.values = append(topicMetadata.values, int16(reqT.version()))
	topicMetadata.values = append(topicMetadata.values, int32(0))
	topicMetadata.values = append(topicMetadata.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, "topic1")
	arr = append(arr, "topic2")
	topicMetadata.values = append(topicMetadata.values, arr)
	topicMetadata.values = append(topicMetadata.values, int32(0))

	encoded, err := EncodeSchema(&topicMetadata, schemaDeleteTopics)

	if err != nil {
		t.Errorf("Test failed, encoding schema %s", err)
	}

	req := &Request{Body: reqT}
	if err = Decode(encoded, req); err != nil {
		t.Errorf("Test failed decoding request, %s", err)
	}

	if len(reqT.GetTopics()) != len(arr) {
		t.Errorf("Number of topics after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
	}

	for i, val := range reqT.GetTopics() {
		if val != arr[i] {
			t.Errorf("Topics gathered after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
		}
	}
}

func TestDeleteTopicsRequestV3(t *testing.T) {
	schemaDeleteTopics := NewSchemaStruct("delete_topics_v3",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topic", ty: typeStr},
		&field{name: "timeout_ms", ty: typeInt32},
	)

	topicMetadata := Struct{
		schema: schemaDeleteTopics,
		values: make([]interface{}, 0),
	}

	reqT := &DeleteTopicsRequestV3{}

	topicMetadata.values = append(topicMetadata.values, int16(reqT.key()))
	topicMetadata.values = append(topicMetadata.values, int16(reqT.version()))
	topicMetadata.values = append(topicMetadata.values, int32(0))
	topicMetadata.values = append(topicMetadata.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, "topic1")
	arr = append(arr, "topic2")
	topicMetadata.values = append(topicMetadata.values, arr)
	topicMetadata.values = append(topicMetadata.values, int32(0))

	encoded, err := EncodeSchema(&topicMetadata, schemaDeleteTopics)

	if err != nil {
		t.Errorf("Test failed, encoding schema %s", err)
	}

	req := &Request{Body: reqT}
	if err = Decode(encoded, req); err != nil {
		t.Errorf("Test failed decoding request, %s", err)
	}

	if len(reqT.GetTopics()) != len(arr) {
		t.Errorf("Number of topics after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
	}

	for i, val := range reqT.GetTopics() {
		if val != arr[i] {
			t.Errorf("Topics gathered after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
		}
	}
}
