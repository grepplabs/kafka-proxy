package protocol

import (
	"testing"
)

func TestCreatePartitionsV0(t *testing.T) {
	assignmentSchema := NewSchemaStruct("assignment_schema",
		&array{name: "broker_ids", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&field{name: "count", ty: typeInt32},
		&array{name: "assignments", ty: assignmentSchema},
	)

	schemaFetch := NewSchemaStruct("createpartitions_v0",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topics", ty: topicSchema},
		&field{name: "timeout_ms", ty: typeInt32},
		&field{name: "validate_only", ty: typeBool},
	)

	assignData := Struct{
		schema: assignmentSchema,
		values: make([]interface{}, 0),
	}

	topicData1 := Struct{
		schema: topicSchema,
		values: make([]interface{}, 0),
	}

	topicData2 := Struct{
		schema: topicSchema,
		values: make([]interface{}, 0),
	}

	createPartitionsData := Struct{
		schema: schemaFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	brokerIds := make([]interface{}, 0)
	brokerIds = append(brokerIds, int32(0))
	brokerIds = append(brokerIds, int32(1))
	assignData.values = append(assignData.values, brokerIds)

	topicData1.values = append(topicData1.values, topics[0])
	topicData1.values = append(topicData1.values, int32(0))
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &assignData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, int32(0))
	topicData2.values = append(topicData2.values, partArr)

	reqT := &CreatePartitionsRequestV0{}

	createPartitionsData.values = append(createPartitionsData.values, int16(reqT.key()))
	createPartitionsData.values = append(createPartitionsData.values, int16(reqT.version()))
	createPartitionsData.values = append(createPartitionsData.values, int32(0))
	createPartitionsData.values = append(createPartitionsData.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	createPartitionsData.values = append(createPartitionsData.values, arr)
	createPartitionsData.values = append(createPartitionsData.values, int32(0))
	createPartitionsData.values = append(createPartitionsData.values, false)

	encoded, err := EncodeSchema(&createPartitionsData, schemaFetch)

	if err != nil {
		t.Fatalf("Test failed, encoding schema %s", err)
	}

	req := &Request{Body: reqT}
	if err = Decode(encoded, req); err != nil {
		t.Fatalf("Test failed decoding request, %s", err)
	}

	if len(reqT.GetTopics()) != len(topics) {
		t.Fatalf("Number of topics after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
	}

	for i, val := range reqT.GetTopics() {
		if val != topics[i] {
			t.Fatalf("Topics gathered after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
		}
	}
}

func TestCreatePartitionsV1(t *testing.T) {
	assignmentSchema := NewSchemaStruct("assignment_schema",
		&array{name: "broker_ids", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&field{name: "count", ty: typeInt32},
		&array{name: "assignments", ty: assignmentSchema},
	)

	schemaFetch := NewSchemaStruct("createpartitions_v1",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&array{name: "topics", ty: topicSchema},
		&field{name: "timeout_ms", ty: typeInt32},
		&field{name: "validate_only", ty: typeBool},
	)

	assignData := Struct{
		schema: assignmentSchema,
		values: make([]interface{}, 0),
	}

	topicData1 := Struct{
		schema: topicSchema,
		values: make([]interface{}, 0),
	}

	topicData2 := Struct{
		schema: topicSchema,
		values: make([]interface{}, 0),
	}

	createPartitionsData := Struct{
		schema: schemaFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	brokerIds := make([]interface{}, 0)
	brokerIds = append(brokerIds, int32(0))
	brokerIds = append(brokerIds, int32(1))
	assignData.values = append(assignData.values, brokerIds)

	topicData1.values = append(topicData1.values, topics[0])
	topicData1.values = append(topicData1.values, int32(0))
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &assignData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, int32(0))
	topicData2.values = append(topicData2.values, partArr)

	reqT := &CreatePartitionsRequestV1{}

	createPartitionsData.values = append(createPartitionsData.values, int16(reqT.key()))
	createPartitionsData.values = append(createPartitionsData.values, int16(reqT.version()))
	createPartitionsData.values = append(createPartitionsData.values, int32(0))
	createPartitionsData.values = append(createPartitionsData.values, "custom")
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	createPartitionsData.values = append(createPartitionsData.values, arr)
	createPartitionsData.values = append(createPartitionsData.values, int32(0))
	createPartitionsData.values = append(createPartitionsData.values, false)

	encoded, err := EncodeSchema(&createPartitionsData, schemaFetch)

	if err != nil {
		t.Fatalf("Test failed, encoding schema %s", err)
	}

	req := &Request{Body: reqT}
	if err = Decode(encoded, req); err != nil {
		t.Fatalf("Test failed decoding request, %s", err)
	}

	if len(reqT.GetTopics()) != len(topics) {
		t.Fatalf("Number of topics after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
	}

	for i, val := range reqT.GetTopics() {
		if val != topics[i] {
			t.Fatalf("Topics gathered after decoding not same as tested, decoded: %v, test_input: %v", reqT.GetTopics(), arr)
		}
	}
}
