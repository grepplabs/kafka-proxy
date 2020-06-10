package protocol

import (
	"testing"
)

func TestOffsetCommitRequestV0(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition_index", ty: typeInt32},
		&field{name: "committed_offset", ty: typeInt64},
		&field{name: "committed_metadata", ty: typeNullableStr},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaOffsetCommit := NewSchemaStruct("offsetcommit_v0",
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

	offsetcommitData := Struct{
		schema: schemaOffsetCommit,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	commMet := "fff"
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, &commMet)

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &OffsetCommitRequestV0{}

	offsetcommitData.values = append(offsetcommitData.values, int16(reqT.key()))
	offsetcommitData.values = append(offsetcommitData.values, int16(reqT.version()))
	offsetcommitData.values = append(offsetcommitData.values, int32(0))
	offsetcommitData.values = append(offsetcommitData.values, "custom")
	offsetcommitData.values = append(offsetcommitData.values, "mygroup")

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	offsetcommitData.values = append(offsetcommitData.values, arr)

	encoded, err := EncodeSchema(&offsetcommitData, schemaOffsetCommit)

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

func TestOffsetCommitRequestV1(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition_index", ty: typeInt32},
		&field{name: "committed_offset", ty: typeInt64},
		&field{name: "commit_timestamp", ty: typeInt64},
		&field{name: "committed_metadata", ty: typeNullableStr},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaOffsetCommit := NewSchemaStruct("offsetcommit_v1",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "group_id", ty: typeStr},
		&field{name: "generation_id", ty: typeInt32},
		&field{name: "member_id", ty: typeStr},
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

	offsetcommitData := Struct{
		schema: schemaOffsetCommit,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	commMet := "fff"
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, &commMet)

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &OffsetCommitRequestV1{}

	offsetcommitData.values = append(offsetcommitData.values, int16(reqT.key()))
	offsetcommitData.values = append(offsetcommitData.values, int16(reqT.version()))
	offsetcommitData.values = append(offsetcommitData.values, int32(0))
	offsetcommitData.values = append(offsetcommitData.values, "custom")
	offsetcommitData.values = append(offsetcommitData.values, "mygroup")
	offsetcommitData.values = append(offsetcommitData.values, int32(0))
	offsetcommitData.values = append(offsetcommitData.values, "mymember")

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	offsetcommitData.values = append(offsetcommitData.values, arr)

	encoded, err := EncodeSchema(&offsetcommitData, schemaOffsetCommit)

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

func TestOffsetCommitRequestV2(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition_index", ty: typeInt32},
		&field{name: "committed_offset", ty: typeInt64},
		&field{name: "committed_metadata", ty: typeNullableStr},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaOffsetCommit := NewSchemaStruct("offsetcommit_v2",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "group_id", ty: typeStr},
		&field{name: "generation_id", ty: typeInt32},
		&field{name: "member_id", ty: typeStr},
		&field{name: "retention_time_ms", ty: typeInt64},
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

	offsetcommitData := Struct{
		schema: schemaOffsetCommit,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	commMet := "fff"
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, &commMet)

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &OffsetCommitRequestV2{}

	offsetcommitData.values = append(offsetcommitData.values, int16(reqT.key()))
	offsetcommitData.values = append(offsetcommitData.values, int16(reqT.version()))
	offsetcommitData.values = append(offsetcommitData.values, int32(0))
	offsetcommitData.values = append(offsetcommitData.values, "custom")
	offsetcommitData.values = append(offsetcommitData.values, "mygroup")
	offsetcommitData.values = append(offsetcommitData.values, int32(0))
	offsetcommitData.values = append(offsetcommitData.values, "mymember")
	offsetcommitData.values = append(offsetcommitData.values, int64(0))

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	offsetcommitData.values = append(offsetcommitData.values, arr)

	encoded, err := EncodeSchema(&offsetcommitData, schemaOffsetCommit)

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

func TestOffsetCommitRequestV3(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition_index", ty: typeInt32},
		&field{name: "committed_offset", ty: typeInt64},
		&field{name: "committed_metadata", ty: typeNullableStr},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaOffsetCommit := NewSchemaStruct("offsetcommit_v3",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "group_id", ty: typeStr},
		&field{name: "generation_id", ty: typeInt32},
		&field{name: "member_id", ty: typeStr},
		&field{name: "retention_time_ms", ty: typeInt64},
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

	offsetcommitData := Struct{
		schema: schemaOffsetCommit,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	commMet := "fff"
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, &commMet)

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &OffsetCommitRequestV3{}

	offsetcommitData.values = append(offsetcommitData.values, int16(reqT.key()))
	offsetcommitData.values = append(offsetcommitData.values, int16(reqT.version()))
	offsetcommitData.values = append(offsetcommitData.values, int32(0))
	offsetcommitData.values = append(offsetcommitData.values, "custom")
	offsetcommitData.values = append(offsetcommitData.values, "mygroup")
	offsetcommitData.values = append(offsetcommitData.values, int32(0))
	offsetcommitData.values = append(offsetcommitData.values, "mymember")
	offsetcommitData.values = append(offsetcommitData.values, int64(0))

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	offsetcommitData.values = append(offsetcommitData.values, arr)

	encoded, err := EncodeSchema(&offsetcommitData, schemaOffsetCommit)

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

func TestOffsetCommitRequestV4(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition_index", ty: typeInt32},
		&field{name: "committed_offset", ty: typeInt64},
		&field{name: "committed_metadata", ty: typeNullableStr},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaOffsetCommit := NewSchemaStruct("offsetcommit_v4",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "group_id", ty: typeStr},
		&field{name: "generation_id", ty: typeInt32},
		&field{name: "member_id", ty: typeStr},
		&field{name: "retention_time_ms", ty: typeInt64},
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

	offsetcommitData := Struct{
		schema: schemaOffsetCommit,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	commMet := "fff"
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, &commMet)

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &OffsetCommitRequestV4{}

	offsetcommitData.values = append(offsetcommitData.values, int16(reqT.key()))
	offsetcommitData.values = append(offsetcommitData.values, int16(reqT.version()))
	offsetcommitData.values = append(offsetcommitData.values, int32(0))
	offsetcommitData.values = append(offsetcommitData.values, "custom")
	offsetcommitData.values = append(offsetcommitData.values, "mygroup")
	offsetcommitData.values = append(offsetcommitData.values, int32(0))
	offsetcommitData.values = append(offsetcommitData.values, "mymember")
	offsetcommitData.values = append(offsetcommitData.values, int64(0))

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	offsetcommitData.values = append(offsetcommitData.values, arr)

	encoded, err := EncodeSchema(&offsetcommitData, schemaOffsetCommit)

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

func TestOffsetCommitRequestV5(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition_index", ty: typeInt32},
		&field{name: "committed_offset", ty: typeInt64},
		&field{name: "committed_metadata", ty: typeNullableStr},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaOffsetCommit := NewSchemaStruct("offsetcommit_v5",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "group_id", ty: typeStr},
		&field{name: "generation_id", ty: typeInt32},
		&field{name: "member_id", ty: typeStr},
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

	offsetcommitData := Struct{
		schema: schemaOffsetCommit,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	commMet := "fff"
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, &commMet)

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &OffsetCommitRequestV5{}

	offsetcommitData.values = append(offsetcommitData.values, int16(reqT.key()))
	offsetcommitData.values = append(offsetcommitData.values, int16(reqT.version()))
	offsetcommitData.values = append(offsetcommitData.values, int32(0))
	offsetcommitData.values = append(offsetcommitData.values, "custom")
	offsetcommitData.values = append(offsetcommitData.values, "mygroup")
	offsetcommitData.values = append(offsetcommitData.values, int32(0))
	offsetcommitData.values = append(offsetcommitData.values, "mymember")

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	offsetcommitData.values = append(offsetcommitData.values, arr)

	encoded, err := EncodeSchema(&offsetcommitData, schemaOffsetCommit)

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

func TestOffsetCommitRequestV6(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition_index", ty: typeInt32},
		&field{name: "committed_offset", ty: typeInt64},
		&field{name: "committed_leader_epoch", ty: typeInt32},
		&field{name: "committed_metadata", ty: typeNullableStr},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaOffsetCommit := NewSchemaStruct("offsetcommit_v6",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "group_id", ty: typeStr},
		&field{name: "generation_id", ty: typeInt32},
		&field{name: "member_id", ty: typeStr},
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

	offsetcommitData := Struct{
		schema: schemaOffsetCommit,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	commMet := "fff"
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, &commMet)

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &OffsetCommitRequestV6{}

	offsetcommitData.values = append(offsetcommitData.values, int16(reqT.key()))
	offsetcommitData.values = append(offsetcommitData.values, int16(reqT.version()))
	offsetcommitData.values = append(offsetcommitData.values, int32(0))
	offsetcommitData.values = append(offsetcommitData.values, "custom")
	offsetcommitData.values = append(offsetcommitData.values, "mygroup")
	offsetcommitData.values = append(offsetcommitData.values, int32(0))
	offsetcommitData.values = append(offsetcommitData.values, "mymember")

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	offsetcommitData.values = append(offsetcommitData.values, arr)

	encoded, err := EncodeSchema(&offsetcommitData, schemaOffsetCommit)

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

func TestOffsetCommitRequestV7(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition_index", ty: typeInt32},
		&field{name: "committed_offset", ty: typeInt64},
		&field{name: "committed_leader_epoch", ty: typeInt32},
		&field{name: "committed_metadata", ty: typeNullableStr},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaOffsetCommit := NewSchemaStruct("offsetcommit_v7",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "group_id", ty: typeStr},
		&field{name: "generation_id", ty: typeInt32},
		&field{name: "member_id", ty: typeStr},
		&field{name: "group_instance_id", ty: typeNullableStr},
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

	offsetcommitData := Struct{
		schema: schemaOffsetCommit,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	commMet := "fff"
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, &commMet)

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &OffsetCommitRequestV7{}

	groupInstId := "bbb"
	offsetcommitData.values = append(offsetcommitData.values, int16(reqT.key()))
	offsetcommitData.values = append(offsetcommitData.values, int16(reqT.version()))
	offsetcommitData.values = append(offsetcommitData.values, int32(0))
	offsetcommitData.values = append(offsetcommitData.values, "custom")
	offsetcommitData.values = append(offsetcommitData.values, "mygroup")
	offsetcommitData.values = append(offsetcommitData.values, int32(0))
	offsetcommitData.values = append(offsetcommitData.values, "mymember")
	offsetcommitData.values = append(offsetcommitData.values, &groupInstId)

	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	offsetcommitData.values = append(offsetcommitData.values, arr)

	encoded, err := EncodeSchema(&offsetcommitData, schemaOffsetCommit)

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
