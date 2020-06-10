package protocol

import (
	"testing"
)

func TestOffsetFetchRequestV0(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition_index", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaOffsetFetch := NewSchemaStruct("offsetfetch_v0",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "group_id", ty: typeStr},
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

	offsetfetchData := Struct{
		schema: schemaOffsetFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	partitionData.values = append(partitionData.values, int32(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &OffsetFetchRequestV0{}

	offsetfetchData.values = append(offsetfetchData.values, int16(reqT.key()))
	offsetfetchData.values = append(offsetfetchData.values, int16(reqT.version()))
	offsetfetchData.values = append(offsetfetchData.values, int32(0))
	offsetfetchData.values = append(offsetfetchData.values, "custom")
	offsetfetchData.values = append(offsetfetchData.values, "mygroup")

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	offsetfetchData.values = append(offsetfetchData.values, arr)

	encoded, err := EncodeSchema(&offsetfetchData, schemaOffsetFetch)

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

func TestOffsetFetchRequestV1(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition_index", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaOffsetFetch := NewSchemaStruct("offsetfetch_v1",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "group_id", ty: typeStr},
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

	offsetfetchData := Struct{
		schema: schemaOffsetFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	partitionData.values = append(partitionData.values, int32(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &OffsetFetchRequestV1{}

	offsetfetchData.values = append(offsetfetchData.values, int16(reqT.key()))
	offsetfetchData.values = append(offsetfetchData.values, int16(reqT.version()))
	offsetfetchData.values = append(offsetfetchData.values, int32(0))
	offsetfetchData.values = append(offsetfetchData.values, "custom")
	offsetfetchData.values = append(offsetfetchData.values, "mygroup")

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	offsetfetchData.values = append(offsetfetchData.values, arr)

	encoded, err := EncodeSchema(&offsetfetchData, schemaOffsetFetch)

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

func TestOffsetFetchRequestV2(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition_index", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaOffsetFetch := NewSchemaStruct("offsetfetch_v2",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "group_id", ty: typeStr},
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

	offsetfetchData := Struct{
		schema: schemaOffsetFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	partitionData.values = append(partitionData.values, int32(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &OffsetFetchRequestV2{}

	offsetfetchData.values = append(offsetfetchData.values, int16(reqT.key()))
	offsetfetchData.values = append(offsetfetchData.values, int16(reqT.version()))
	offsetfetchData.values = append(offsetfetchData.values, int32(0))
	offsetfetchData.values = append(offsetfetchData.values, "custom")
	offsetfetchData.values = append(offsetfetchData.values, "mygroup")

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	offsetfetchData.values = append(offsetfetchData.values, arr)

	encoded, err := EncodeSchema(&offsetfetchData, schemaOffsetFetch)

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

func TestOffsetFetchRequestV3(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition_index", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaOffsetFetch := NewSchemaStruct("offsetfetch_v3",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "group_id", ty: typeStr},
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

	offsetfetchData := Struct{
		schema: schemaOffsetFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	partitionData.values = append(partitionData.values, int32(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &OffsetFetchRequestV3{}

	offsetfetchData.values = append(offsetfetchData.values, int16(reqT.key()))
	offsetfetchData.values = append(offsetfetchData.values, int16(reqT.version()))
	offsetfetchData.values = append(offsetfetchData.values, int32(0))
	offsetfetchData.values = append(offsetfetchData.values, "custom")
	offsetfetchData.values = append(offsetfetchData.values, "mygroup")

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	offsetfetchData.values = append(offsetfetchData.values, arr)

	encoded, err := EncodeSchema(&offsetfetchData, schemaOffsetFetch)

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

func TestOffsetFetchRequestV4(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition_index", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaOffsetFetch := NewSchemaStruct("offsetfetch_v4",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "group_id", ty: typeStr},
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

	offsetfetchData := Struct{
		schema: schemaOffsetFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	partitionData.values = append(partitionData.values, int32(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &OffsetFetchRequestV4{}

	offsetfetchData.values = append(offsetfetchData.values, int16(reqT.key()))
	offsetfetchData.values = append(offsetfetchData.values, int16(reqT.version()))
	offsetfetchData.values = append(offsetfetchData.values, int32(0))
	offsetfetchData.values = append(offsetfetchData.values, "custom")
	offsetfetchData.values = append(offsetfetchData.values, "mygroup")

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	offsetfetchData.values = append(offsetfetchData.values, arr)

	encoded, err := EncodeSchema(&offsetfetchData, schemaOffsetFetch)

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

func TestOffsetFetchRequestV5(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition_index", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaOffsetFetch := NewSchemaStruct("offsetfetch_v5",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "group_id", ty: typeStr},
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

	offsetfetchData := Struct{
		schema: schemaOffsetFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	partitionData.values = append(partitionData.values, int32(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &OffsetFetchRequestV5{}

	offsetfetchData.values = append(offsetfetchData.values, int16(reqT.key()))
	offsetfetchData.values = append(offsetfetchData.values, int16(reqT.version()))
	offsetfetchData.values = append(offsetfetchData.values, int32(0))
	offsetfetchData.values = append(offsetfetchData.values, "custom")
	offsetfetchData.values = append(offsetfetchData.values, "mygroup")

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	offsetfetchData.values = append(offsetfetchData.values, arr)

	encoded, err := EncodeSchema(&offsetfetchData, schemaOffsetFetch)

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
