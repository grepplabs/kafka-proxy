package protocol

import (
	"testing"
)

func TestListOffsetsRequestV0(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&field{name: "timestamp", ty: typeInt64},
		&field{name: "max_num_offsets", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaListOffsets := NewSchemaStruct("listoffsets_v0",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "replica_id", ty: typeInt32},
		&array{name: "topics", ty: topicSchema},
	)

	partitionData := Struct{
		schema: partitionSchema,
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

	listoffsetsData := Struct{
		schema: schemaListOffsets,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, int32(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &ListOffsetsRequestV0{}

	listoffsetsData.values = append(listoffsetsData.values, int16(reqT.key()))
	listoffsetsData.values = append(listoffsetsData.values, int16(reqT.version()))
	listoffsetsData.values = append(listoffsetsData.values, int32(0))
	listoffsetsData.values = append(listoffsetsData.values, "custom")
	listoffsetsData.values = append(listoffsetsData.values, int32(0))

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	listoffsetsData.values = append(listoffsetsData.values, arr)

	encoded, err := EncodeSchema(&listoffsetsData, schemaListOffsets)

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

func TestListOffsetsRequestV1(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&field{name: "timestamp", ty: typeInt64},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaListOffsets := NewSchemaStruct("listoffsets_v1",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "replica_id", ty: typeInt32},
		&array{name: "topics", ty: topicSchema},
	)

	partitionData := Struct{
		schema: partitionSchema,
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

	listoffsetsData := Struct{
		schema: schemaListOffsets,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &ListOffsetsRequestV1{}

	listoffsetsData.values = append(listoffsetsData.values, int16(reqT.key()))
	listoffsetsData.values = append(listoffsetsData.values, int16(reqT.version()))
	listoffsetsData.values = append(listoffsetsData.values, int32(0))
	listoffsetsData.values = append(listoffsetsData.values, "custom")
	listoffsetsData.values = append(listoffsetsData.values, int32(0))

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	listoffsetsData.values = append(listoffsetsData.values, arr)

	encoded, err := EncodeSchema(&listoffsetsData, schemaListOffsets)

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

func TestListOffsetsRequestV2(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&field{name: "timestamp", ty: typeInt64},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaListOffsets := NewSchemaStruct("listoffsets_v2",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "replica_id", ty: typeInt32},
		&field{name: "isolation_level", ty: typeInt8},
		&array{name: "topics", ty: topicSchema},
	)

	partitionData := Struct{
		schema: partitionSchema,
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

	listoffsetsData := Struct{
		schema: schemaListOffsets,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &ListOffsetsRequestV2{}

	listoffsetsData.values = append(listoffsetsData.values, int16(reqT.key()))
	listoffsetsData.values = append(listoffsetsData.values, int16(reqT.version()))
	listoffsetsData.values = append(listoffsetsData.values, int32(0))
	listoffsetsData.values = append(listoffsetsData.values, "custom")
	listoffsetsData.values = append(listoffsetsData.values, int32(0))
	listoffsetsData.values = append(listoffsetsData.values, int8(0))

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	listoffsetsData.values = append(listoffsetsData.values, arr)

	encoded, err := EncodeSchema(&listoffsetsData, schemaListOffsets)

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

func TestListOffsetsRequestV3(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&field{name: "timestamp", ty: typeInt64},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaListOffsets := NewSchemaStruct("listoffsets_v3",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "replica_id", ty: typeInt32},
		&field{name: "isolation_level", ty: typeInt8},
		&array{name: "topics", ty: topicSchema},
	)

	partitionData := Struct{
		schema: partitionSchema,
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

	listoffsetsData := Struct{
		schema: schemaListOffsets,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &ListOffsetsRequestV3{}

	listoffsetsData.values = append(listoffsetsData.values, int16(reqT.key()))
	listoffsetsData.values = append(listoffsetsData.values, int16(reqT.version()))
	listoffsetsData.values = append(listoffsetsData.values, int32(0))
	listoffsetsData.values = append(listoffsetsData.values, "custom")
	listoffsetsData.values = append(listoffsetsData.values, int32(0))
	listoffsetsData.values = append(listoffsetsData.values, int8(0))

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	listoffsetsData.values = append(listoffsetsData.values, arr)

	encoded, err := EncodeSchema(&listoffsetsData, schemaListOffsets)

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

func TestListOffsetsRequestV4(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&field{name: "current_leader_epoch", ty: typeInt32},
		&field{name: "timestamp", ty: typeInt64},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaListOffsets := NewSchemaStruct("listoffsets_v4",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "replica_id", ty: typeInt32},
		&field{name: "isolation_level", ty: typeInt8},
		&array{name: "topics", ty: topicSchema},
	)

	partitionData := Struct{
		schema: partitionSchema,
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

	listoffsetsData := Struct{
		schema: schemaListOffsets,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &ListOffsetsRequestV4{}

	listoffsetsData.values = append(listoffsetsData.values, int16(reqT.key()))
	listoffsetsData.values = append(listoffsetsData.values, int16(reqT.version()))
	listoffsetsData.values = append(listoffsetsData.values, int32(0))
	listoffsetsData.values = append(listoffsetsData.values, "custom")
	listoffsetsData.values = append(listoffsetsData.values, int32(0))
	listoffsetsData.values = append(listoffsetsData.values, int8(0))

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	listoffsetsData.values = append(listoffsetsData.values, arr)

	encoded, err := EncodeSchema(&listoffsetsData, schemaListOffsets)

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

func TestListOffsetsRequestV5(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&field{name: "current_leader_epoch", ty: typeInt32},
		&field{name: "timestamp", ty: typeInt64},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaListOffsets := NewSchemaStruct("listoffsets_v5",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "replica_id", ty: typeInt32},
		&field{name: "isolation_level", ty: typeInt8},
		&array{name: "topics", ty: topicSchema},
	)

	partitionData := Struct{
		schema: partitionSchema,
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

	listoffsetsData := Struct{
		schema: schemaListOffsets,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &ListOffsetsRequestV5{}

	listoffsetsData.values = append(listoffsetsData.values, int16(reqT.key()))
	listoffsetsData.values = append(listoffsetsData.values, int16(reqT.version()))
	listoffsetsData.values = append(listoffsetsData.values, int32(0))
	listoffsetsData.values = append(listoffsetsData.values, "custom")
	listoffsetsData.values = append(listoffsetsData.values, int32(0))
	listoffsetsData.values = append(listoffsetsData.values, int8(0))

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	listoffsetsData.values = append(listoffsetsData.values, arr)

	encoded, err := EncodeSchema(&listoffsetsData, schemaListOffsets)

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
