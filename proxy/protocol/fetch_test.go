package protocol

import (
	"testing"
)

func TestFetchRequestV0(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&field{name: "fetch_offset", ty: typeInt64},
		&field{name: "partition_max_bytes", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaFetch := NewSchemaStruct("fetch_v0",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "reaplica_id", ty: typeInt32},
		&field{name: "max_wait_time", ty: typeInt32},
		&field{name: "min_bytes", ty: typeInt32},
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

	fetchData := Struct{
		schema: schemaFetch,
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

	reqT := &FetchRequestV0{}

	fetchData.values = append(fetchData.values, int16(reqT.key()))
	fetchData.values = append(fetchData.values, int16(reqT.version()))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, "custom")
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	fetchData.values = append(fetchData.values, arr)

	encoded, err := EncodeSchema(&fetchData, schemaFetch)

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

func TestFetchRequestV1(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&field{name: "fetch_offset", ty: typeInt64},
		&field{name: "partition_max_bytes", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaFetch := NewSchemaStruct("fetch_v1",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "reaplica_id", ty: typeInt32},
		&field{name: "max_wait_time", ty: typeInt32},
		&field{name: "min_bytes", ty: typeInt32},
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

	fetchData := Struct{
		schema: schemaFetch,
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

	reqT := &FetchRequestV1{}

	fetchData.values = append(fetchData.values, int16(reqT.key()))
	fetchData.values = append(fetchData.values, int16(reqT.version()))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, "custom")
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	fetchData.values = append(fetchData.values, arr)

	encoded, err := EncodeSchema(&fetchData, schemaFetch)

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

func TestFetchRequestV2(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&field{name: "fetch_offset", ty: typeInt64},
		&field{name: "partition_max_bytes", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaFetch := NewSchemaStruct("fetch_v2",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "reaplica_id", ty: typeInt32},
		&field{name: "max_wait_time", ty: typeInt32},
		&field{name: "min_bytes", ty: typeInt32},
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

	fetchData := Struct{
		schema: schemaFetch,
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

	reqT := &FetchRequestV2{}

	fetchData.values = append(fetchData.values, int16(reqT.key()))
	fetchData.values = append(fetchData.values, int16(reqT.version()))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, "custom")
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	fetchData.values = append(fetchData.values, arr)

	encoded, err := EncodeSchema(&fetchData, schemaFetch)

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

func TestFetchRequestV3(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&field{name: "fetch_offset", ty: typeInt64},
		&field{name: "partition_max_bytes", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaFetch := NewSchemaStruct("fetch_v3",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "reaplica_id", ty: typeInt32},
		&field{name: "max_wait_time", ty: typeInt32},
		&field{name: "min_bytes", ty: typeInt32},
		&field{name: "max_bytes", ty: typeInt32},
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

	fetchData := Struct{
		schema: schemaFetch,
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

	reqT := &FetchRequestV3{}

	fetchData.values = append(fetchData.values, int16(reqT.key()))
	fetchData.values = append(fetchData.values, int16(reqT.version()))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, "custom")
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	fetchData.values = append(fetchData.values, arr)

	encoded, err := EncodeSchema(&fetchData, schemaFetch)

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

func TestFetchRequestV4(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&field{name: "fetch_offset", ty: typeInt64},
		&field{name: "partition_max_bytes", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaFetch := NewSchemaStruct("fetch_v4",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "reaplica_id", ty: typeInt32},
		&field{name: "max_wait_time", ty: typeInt32},
		&field{name: "min_bytes", ty: typeInt32},
		&field{name: "max_bytes", ty: typeInt32},
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

	fetchData := Struct{
		schema: schemaFetch,
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

	reqT := &FetchRequestV4{}

	fetchData.values = append(fetchData.values, int16(reqT.key()))
	fetchData.values = append(fetchData.values, int16(reqT.version()))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, "custom")
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int8(0))
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	fetchData.values = append(fetchData.values, arr)

	encoded, err := EncodeSchema(&fetchData, schemaFetch)

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

func TestFetchRequestV5(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&field{name: "fetch_offset", ty: typeInt64},
		&field{name: "log_start_offset", ty: typeInt64},
		&field{name: "partition_max_bytes", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaFetch := NewSchemaStruct("fetch_v5",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "reaplica_id", ty: typeInt32},
		&field{name: "max_wait_time", ty: typeInt32},
		&field{name: "min_bytes", ty: typeInt32},
		&field{name: "max_bytes", ty: typeInt32},
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

	fetchData := Struct{
		schema: schemaFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, int32(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &FetchRequestV5{}

	fetchData.values = append(fetchData.values, int16(reqT.key()))
	fetchData.values = append(fetchData.values, int16(reqT.version()))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, "custom")
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int8(0))
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	fetchData.values = append(fetchData.values, arr)

	encoded, err := EncodeSchema(&fetchData, schemaFetch)

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

func TestFetchRequestV6(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&field{name: "fetch_offset", ty: typeInt64},
		&field{name: "log_start_offset", ty: typeInt64},
		&field{name: "partition_max_bytes", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	schemaFetch := NewSchemaStruct("fetch_v6",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "reaplica_id", ty: typeInt32},
		&field{name: "max_wait_time", ty: typeInt32},
		&field{name: "min_bytes", ty: typeInt32},
		&field{name: "max_bytes", ty: typeInt32},
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

	fetchData := Struct{
		schema: schemaFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, int32(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &FetchRequestV6{}

	fetchData.values = append(fetchData.values, int16(reqT.key()))
	fetchData.values = append(fetchData.values, int16(reqT.version()))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, "custom")
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int8(0))
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	fetchData.values = append(fetchData.values, arr)

	encoded, err := EncodeSchema(&fetchData, schemaFetch)

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

func TestFetchRequestV7(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&field{name: "fetch_offset", ty: typeInt64},
		&field{name: "log_start_offset", ty: typeInt64},
		&field{name: "partition_max_bytes", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	forgotTopicSchema := NewSchemaStruct("forgot_topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: typeInt32},
	)

	schemaFetch := NewSchemaStruct("fetch_v7",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "reaplica_id", ty: typeInt32},
		&field{name: "max_wait_time", ty: typeInt32},
		&field{name: "min_bytes", ty: typeInt32},
		&field{name: "max_bytes", ty: typeInt32},
		&field{name: "isolation_level", ty: typeInt8},
		&field{name: "session_id", ty: typeInt32},
		&field{name: "session_epoch", ty: typeInt32},
		&array{name: "topics", ty: topicSchema},
		&array{name: "forgot_topics", ty: forgotTopicSchema},
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

	forgotTopicData := Struct{
		schema: forgotTopicSchema,
		values: make([]interface{}, 0),
	}

	fetchData := Struct{
		schema: schemaFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	forgotPartArr := make([]interface{}, 0)
	forgotPartArr = append(forgotPartArr, int32(0))
	forgotTopicData.values = append(forgotTopicData.values, "forgot_topic")
	forgotTopicData.values = append(forgotTopicData.values, forgotPartArr)

	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, int32(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &FetchRequestV7{}

	fetchData.values = append(fetchData.values, int16(reqT.key()))
	fetchData.values = append(fetchData.values, int16(reqT.version()))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, "custom")
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int8(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	fetchData.values = append(fetchData.values, arr)
	forgotTopics := make([]interface{}, 0)
	forgotTopics = append(forgotTopics, &forgotTopicData)
	fetchData.values = append(fetchData.values, forgotTopics)

	encoded, err := EncodeSchema(&fetchData, schemaFetch)

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

func TestFetchRequestV9(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&field{name: "current_leader_epoch", ty: typeInt32},
		&field{name: "fetch_offset", ty: typeInt64},
		&field{name: "log_start_offset", ty: typeInt64},
		&field{name: "partition_max_bytes", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	forgotTopicSchema := NewSchemaStruct("forgot_topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: typeInt32},
	)

	schemaFetch := NewSchemaStruct("fetch_v9",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "reaplica_id", ty: typeInt32},
		&field{name: "max_wait_time", ty: typeInt32},
		&field{name: "min_bytes", ty: typeInt32},
		&field{name: "max_bytes", ty: typeInt32},
		&field{name: "isolation_level", ty: typeInt8},
		&field{name: "session_id", ty: typeInt32},
		&field{name: "session_epoch", ty: typeInt32},
		&array{name: "topics", ty: topicSchema},
		&array{name: "forgot_topics", ty: forgotTopicSchema},
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

	forgotTopicData := Struct{
		schema: forgotTopicSchema,
		values: make([]interface{}, 0),
	}

	fetchData := Struct{
		schema: schemaFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	forgotPartArr := make([]interface{}, 0)
	forgotPartArr = append(forgotPartArr, int32(0))
	forgotTopicData.values = append(forgotTopicData.values, "forgot_topic")
	forgotTopicData.values = append(forgotTopicData.values, forgotPartArr)

	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, int32(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &FetchRequestV9{}

	fetchData.values = append(fetchData.values, int16(reqT.key()))
	fetchData.values = append(fetchData.values, int16(reqT.version()))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, "custom")
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int8(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	fetchData.values = append(fetchData.values, arr)
	forgotTopics := make([]interface{}, 0)
	forgotTopics = append(forgotTopics, &forgotTopicData)
	fetchData.values = append(fetchData.values, forgotTopics)

	encoded, err := EncodeSchema(&fetchData, schemaFetch)

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

func TestFetchRequestV10(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&field{name: "current_leader_epoch", ty: typeInt32},
		&field{name: "fetch_offset", ty: typeInt64},
		&field{name: "log_start_offset", ty: typeInt64},
		&field{name: "partition_max_bytes", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	forgotTopicSchema := NewSchemaStruct("forgot_topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: typeInt32},
	)

	schemaFetch := NewSchemaStruct("fetch_v10",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "reaplica_id", ty: typeInt32},
		&field{name: "max_wait_time", ty: typeInt32},
		&field{name: "min_bytes", ty: typeInt32},
		&field{name: "max_bytes", ty: typeInt32},
		&field{name: "isolation_level", ty: typeInt8},
		&field{name: "session_id", ty: typeInt32},
		&field{name: "session_epoch", ty: typeInt32},
		&array{name: "topics", ty: topicSchema},
		&array{name: "forgot_topics", ty: forgotTopicSchema},
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

	forgotTopicData := Struct{
		schema: forgotTopicSchema,
		values: make([]interface{}, 0),
	}

	fetchData := Struct{
		schema: schemaFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	forgotPartArr := make([]interface{}, 0)
	forgotPartArr = append(forgotPartArr, int32(0))
	forgotTopicData.values = append(forgotTopicData.values, "forgot_topic")
	forgotTopicData.values = append(forgotTopicData.values, forgotPartArr)

	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, int32(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &FetchRequestV10{}

	fetchData.values = append(fetchData.values, int16(reqT.key()))
	fetchData.values = append(fetchData.values, int16(reqT.version()))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, "custom")
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int8(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	fetchData.values = append(fetchData.values, arr)
	forgotTopics := make([]interface{}, 0)
	forgotTopics = append(forgotTopics, &forgotTopicData)
	fetchData.values = append(fetchData.values, forgotTopics)

	encoded, err := EncodeSchema(&fetchData, schemaFetch)

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

func TestFetchRequestV11(t *testing.T) {
	partitionSchema := NewSchemaStruct("partition_schema",
		&field{name: "partition", ty: typeInt32},
		&field{name: "current_leader_epoch", ty: typeInt32},
		&field{name: "fetch_offset", ty: typeInt64},
		&field{name: "log_start_offset", ty: typeInt64},
		&field{name: "partition_max_bytes", ty: typeInt32},
	)

	topicSchema := NewSchemaStruct("topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: partitionSchema},
	)

	forgotTopicSchema := NewSchemaStruct("forgot_topic_schema",
		&field{name: "topic_name", ty: typeStr},
		&array{name: "partitions", ty: typeInt32},
	)

	schemaFetch := NewSchemaStruct("fetch_v11",
		&field{name: "key", ty: typeInt16},
		&field{name: "version", ty: typeInt16},
		&field{name: "correlation_id", ty: typeInt32},
		&field{name: "client_id", ty: typeStr},
		&field{name: "reaplica_id", ty: typeInt32},
		&field{name: "max_wait_time", ty: typeInt32},
		&field{name: "min_bytes", ty: typeInt32},
		&field{name: "max_bytes", ty: typeInt32},
		&field{name: "isolation_level", ty: typeInt8},
		&field{name: "session_id", ty: typeInt32},
		&field{name: "session_epoch", ty: typeInt32},
		&array{name: "topics", ty: topicSchema},
		&array{name: "forgot_topics", ty: forgotTopicSchema},
		&field{name: "rack_id", ty: typeStr},
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

	forgotTopicData := Struct{
		schema: forgotTopicSchema,
		values: make([]interface{}, 0),
	}

	fetchData := Struct{
		schema: schemaFetch,
		values: make([]interface{}, 0),
	}

	topics := []string{"topic1", "topic2"}
	forgotPartArr := make([]interface{}, 0)
	forgotPartArr = append(forgotPartArr, int32(0))
	forgotTopicData.values = append(forgotTopicData.values, "forgot_topic")
	forgotTopicData.values = append(forgotTopicData.values, forgotPartArr)

	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int32(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, int64(0))
	partitionData.values = append(partitionData.values, int32(0))

	topicData1.values = append(topicData1.values, topics[0])
	partArr := make([]interface{}, 0)
	partArr = append(partArr, &partitionData)
	topicData1.values = append(topicData1.values, partArr)

	topicData2.values = append(topicData2.values, topics[1])
	topicData2.values = append(topicData2.values, partArr)

	reqT := &FetchRequestV11{}

	fetchData.values = append(fetchData.values, int16(reqT.key()))
	fetchData.values = append(fetchData.values, int16(reqT.version()))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, "custom")
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int8(0))
	fetchData.values = append(fetchData.values, int32(0))
	fetchData.values = append(fetchData.values, int32(0))
	arr := make([]interface{}, 0)
	arr = append(arr, &topicData1)
	arr = append(arr, &topicData2)
	fetchData.values = append(fetchData.values, arr)
	forgotTopics := make([]interface{}, 0)
	forgotTopics = append(forgotTopics, &forgotTopicData)
	fetchData.values = append(fetchData.values, forgotTopics)
	fetchData.values = append(fetchData.values, "rack1")

	encoded, err := EncodeSchema(&fetchData, schemaFetch)

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
