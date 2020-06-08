package protocol

import (
	"testing"
)

func TestTopicMetadataRequestV0(t *testing.T) {
	schemaTopicMetadata := NewSchemaStruct("topic_metadata_v0",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topic", ty: typeStr},
	)

	topicMetadata := Struct{
		schema: schemaTopicMetadata,
		values: make([]interface{}, 0),
	}

	reqT := &TopicMetadataRequestV0{}

	topicMetadata.values = append(topicMetadata.values, int16(reqT.key()))
	topicMetadata.values = append(topicMetadata.values, int16(reqT.version()))
	topicMetadata.values = append(topicMetadata.values, int32(0))
	topicMetadata.values = append(topicMetadata.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, "topic1")
	arr = append(arr, "topic2")
	topicMetadata.values = append(topicMetadata.values, arr)

	encoded, err := EncodeSchema(&topicMetadata, schemaTopicMetadata)

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

func TestTopicMetadataRequestV1(t *testing.T) {
	schemaTopicMetadata := NewSchemaStruct("topic_metadata_v1",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topic", ty: typeStr},
	)

	topicMetadata := Struct{
		schema: schemaTopicMetadata,
		values: make([]interface{}, 0),
	}

	reqT := &TopicMetadataRequestV1{}

	topicMetadata.values = append(topicMetadata.values, int16(reqT.key()))
	topicMetadata.values = append(topicMetadata.values, int16(reqT.version()))
	topicMetadata.values = append(topicMetadata.values, int32(0))
	topicMetadata.values = append(topicMetadata.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, "topic1")
	arr = append(arr, "topic2")
	topicMetadata.values = append(topicMetadata.values, arr)

	encoded, err := EncodeSchema(&topicMetadata, schemaTopicMetadata)

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

func TestTopicMetadataRequestV2(t *testing.T) {
	schemaTopicMetadata := NewSchemaStruct("topic_metadata_v2",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topic", ty: typeStr},
	)

	topicMetadata := Struct{
		schema: schemaTopicMetadata,
		values: make([]interface{}, 0),
	}

	reqT := &TopicMetadataRequestV2{}

	topicMetadata.values = append(topicMetadata.values, int16(reqT.key()))
	topicMetadata.values = append(topicMetadata.values, int16(reqT.version()))
	topicMetadata.values = append(topicMetadata.values, int32(0))
	topicMetadata.values = append(topicMetadata.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, "topic1")
	arr = append(arr, "topic2")
	topicMetadata.values = append(topicMetadata.values, arr)

	encoded, err := EncodeSchema(&topicMetadata, schemaTopicMetadata)

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

func TestTopicMetadataRequestV3(t *testing.T) {
	schemaTopicMetadata := NewSchemaStruct("topic_metadata_v3",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topic", ty: typeStr},
	)

	topicMetadata := Struct{
		schema: schemaTopicMetadata,
		values: make([]interface{}, 0),
	}

	reqT := &TopicMetadataRequestV3{}

	topicMetadata.values = append(topicMetadata.values, int16(reqT.key()))
	topicMetadata.values = append(topicMetadata.values, int16(reqT.version()))
	topicMetadata.values = append(topicMetadata.values, int32(0))
	topicMetadata.values = append(topicMetadata.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, "topic1")
	arr = append(arr, "topic2")
	topicMetadata.values = append(topicMetadata.values, arr)

	encoded, err := EncodeSchema(&topicMetadata, schemaTopicMetadata)

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

func TestTopicMetadataRequestV4(t *testing.T) {
	schemaTopicMetadata := NewSchemaStruct("topic_metadata_v4",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topic", ty: typeStr},
		&field{name: "allow_auto_topic_creation", ty: typeBool},
	)

	topicMetadata := Struct{
		schema: schemaTopicMetadata,
		values: make([]interface{}, 0),
	}

	reqT := &TopicMetadataRequestV4{}

	topicMetadata.values = append(topicMetadata.values, int16(reqT.key()))
	topicMetadata.values = append(topicMetadata.values, int16(reqT.version()))
	topicMetadata.values = append(topicMetadata.values, int32(0))
	topicMetadata.values = append(topicMetadata.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, "topic1")
	arr = append(arr, "topic2")
	topicMetadata.values = append(topicMetadata.values, arr)
	topicMetadata.values = append(topicMetadata.values, false)

	encoded, err := EncodeSchema(&topicMetadata, schemaTopicMetadata)

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

func TestTopicMetadataRequestV5(t *testing.T) {
	schemaTopicMetadata := NewSchemaStruct("topic_metadata_v5",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topic", ty: typeStr},
		&field{name: "allow_auto_topic_creation", ty: typeBool},
	)

	topicMetadata := Struct{
		schema: schemaTopicMetadata,
		values: make([]interface{}, 0),
	}

	reqT := &TopicMetadataRequestV5{}

	topicMetadata.values = append(topicMetadata.values, int16(reqT.key()))
	topicMetadata.values = append(topicMetadata.values, int16(reqT.version()))
	topicMetadata.values = append(topicMetadata.values, int32(0))
	topicMetadata.values = append(topicMetadata.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, "topic1")
	arr = append(arr, "topic2")
	topicMetadata.values = append(topicMetadata.values, arr)
	topicMetadata.values = append(topicMetadata.values, false)

	encoded, err := EncodeSchema(&topicMetadata, schemaTopicMetadata)

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

func TestTopicMetadataRequestV6(t *testing.T) {
	schemaTopicMetadata := NewSchemaStruct("topic_metadata_v6",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topic", ty: typeStr},
		&field{name: "allow_auto_topic_creation", ty: typeBool},
	)

	topicMetadata := Struct{
		schema: schemaTopicMetadata,
		values: make([]interface{}, 0),
	}

	reqT := &TopicMetadataRequestV6{}

	topicMetadata.values = append(topicMetadata.values, int16(reqT.key()))
	topicMetadata.values = append(topicMetadata.values, int16(reqT.version()))
	topicMetadata.values = append(topicMetadata.values, int32(0))
	topicMetadata.values = append(topicMetadata.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, "topic1")
	arr = append(arr, "topic2")
	topicMetadata.values = append(topicMetadata.values, arr)
	topicMetadata.values = append(topicMetadata.values, false)

	encoded, err := EncodeSchema(&topicMetadata, schemaTopicMetadata)

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

func TestTopicMetadataRequestV7(t *testing.T) {
	schemaTopicMetadata := NewSchemaStruct("topic_metadata_v7",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topic", ty: typeStr},
		&field{name: "allow_auto_topic_creation", ty: typeBool},
	)

	topicMetadata := Struct{
		schema: schemaTopicMetadata,
		values: make([]interface{}, 0),
	}

	reqT := &TopicMetadataRequestV7{}

	topicMetadata.values = append(topicMetadata.values, int16(reqT.key()))
	topicMetadata.values = append(topicMetadata.values, int16(reqT.version()))
	topicMetadata.values = append(topicMetadata.values, int32(0))
	topicMetadata.values = append(topicMetadata.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, "topic1")
	arr = append(arr, "topic2")
	topicMetadata.values = append(topicMetadata.values, arr)
	topicMetadata.values = append(topicMetadata.values, false)

	encoded, err := EncodeSchema(&topicMetadata, schemaTopicMetadata)

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

func TestTopicMetadataRequestV8(t *testing.T) {
	schemaTopicMetadata := NewSchemaStruct("topic_metadata_v8",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topic", ty: typeStr},
		&field{name: "allow_auto_topic_creation", ty: typeBool},
		&field{name: "include_cluster_authorized_operations", ty: typeBool},
		&field{name: "include_topic_authorized_operations", ty: typeBool},
	)

	topicMetadata := Struct{
		schema: schemaTopicMetadata,
		values: make([]interface{}, 0),
	}

	reqT := &TopicMetadataRequestV8{}

	topicMetadata.values = append(topicMetadata.values, int16(reqT.key()))
	topicMetadata.values = append(topicMetadata.values, int16(reqT.version()))
	topicMetadata.values = append(topicMetadata.values, int32(0))
	topicMetadata.values = append(topicMetadata.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, "topic1")
	arr = append(arr, "topic2")
	topicMetadata.values = append(topicMetadata.values, arr)
	topicMetadata.values = append(topicMetadata.values, false)
	topicMetadata.values = append(topicMetadata.values, false)
	topicMetadata.values = append(topicMetadata.values, false)

	encoded, err := EncodeSchema(&topicMetadata, schemaTopicMetadata)

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
