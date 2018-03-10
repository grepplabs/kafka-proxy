package protocol

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

var (
	emptyMetadataResponse = []byte{
		// brokers
		0x00, 0x00, 0x00, 0x00,
		// topic_metadata
		0x00, 0x00, 0x00, 0x00}
)

func TestDecodeEmptyMetadataResponseV0(t *testing.T) {
	a := assert.New(t)

	schema := metadataResponseSchemaVersions[0]
	bytes := emptyMetadataResponse

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	a.Equal(`metadata_response_v0{brokers:[],topic_metadata:[]}`, s.String())

	a.Len(s.values, 2)

	brokers, ok := s.Get("brokers").([]interface{})
	a.True(ok)
	a.Len(brokers, 0)

	topicMetadata, ok := s.Get("topic_metadata").([]interface{})
	a.True(ok)
	a.Len(topicMetadata, 0)

	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)
}

func TestMetadataResponseV0WithBrokers(t *testing.T) {
	// The Hard Way
	bytes := []byte{
		// brokers
		0x00, 0x00, 0x00, 0x02,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273

		// topic_metadata
		0x00, 0x00, 0x00, 0x00}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[0]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	a.Equal(`metadata_response_v0{brokers:[metadata_broker_v0{node_id:44031,host:localhost,port:51} metadata_broker_v0{node_id:66051,host:google.com,port:273}],topic_metadata:[]}`, s.String())
	a.Len(s.values, 2)

	brokers, ok := s.Get("brokers").([]interface{})
	a.True(ok)
	a.Len(brokers, 2)

	// broker[0]
	brokerStruct, ok := brokers[0].(*Struct)
	a.True(ok)
	a.Len(brokerStruct.values, 3)

	nodeId, ok := brokerStruct.Get("node_id").(int32)
	a.True(ok)
	a.Equal(int32(0xabff), nodeId)

	host, ok := brokerStruct.Get("host").(string)
	a.True(ok)
	a.Equal("localhost", host)

	port, ok := brokerStruct.Get("port").(int32)
	a.True(ok)
	a.Equal(int32(0x33), port)

	// broker[1]
	brokerStruct, ok = brokers[1].(*Struct)
	a.True(ok)
	a.Len(brokerStruct.values, 3)

	nodeId, ok = brokerStruct.Get("node_id").(int32)
	a.True(ok)
	a.Equal(int32(0x00010203), nodeId)

	host, ok = brokerStruct.Get("host").(string)
	a.True(ok)
	a.Equal("google.com", host)

	port, ok = brokerStruct.Get("port").(int32)
	a.True(ok)
	a.Equal(int32(0x0111), port)

	topicMetadata, ok := s.Get("topic_metadata").([]interface{})
	a.True(ok)
	a.Len(topicMetadata, 0)

	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)
}

func TestMetadataResponseV0WithTopics(t *testing.T) {
	// The Hard Way

	bytes := []byte{
		// brokers
		0x00, 0x00, 0x00, 0x00,
		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x00,

		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		// partition_metadata
		0x00, 0x00, 0x00, 0x00}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[0]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	a.Equal(`metadata_response_v0{brokers:[],topic_metadata:[topic_metadata_v0{error_code:0,topic:foo,partition_metadata:[partition_metadata_v0{error_code:4,partition:1,leader:7,replicas:[1 2 3],isr:[]}]} topic_metadata_v0{error_code:0,topic:bar,partition_metadata:[]}]}`, s.String())
	a.Len(s.values, 2)

	brokers, ok := s.Get("brokers").([]interface{})
	a.True(ok)
	a.Len(brokers, 0)

	topicMetadata, ok := s.Get("topic_metadata").([]interface{})
	a.True(ok)
	a.Len(topicMetadata, 2)

	// topic_metadata[0]
	topicMetadataStruct, ok := topicMetadata[0].(*Struct)
	a.True(ok)
	a.Len(topicMetadataStruct.values, 3)

	errorCode, ok := topicMetadataStruct.Get("error_code").(int16)
	a.True(ok)
	a.Equal(int16(0), errorCode)

	topic, ok := topicMetadataStruct.Get("topic").(string)
	a.True(ok)
	a.Equal("foo", topic)

	partitionMetadata, ok := topicMetadataStruct.Get("partition_metadata").([]interface{})
	a.True(ok)
	a.Len(partitionMetadata, 1)

	// partitionMetadata[0]
	partitionMetadataStruct, ok := partitionMetadata[0].(*Struct)
	a.True(ok)
	a.Len(partitionMetadataStruct.values, 5)

	errorCode, ok = partitionMetadataStruct.Get("error_code").(int16)
	a.True(ok)
	a.Equal(int16(4), errorCode)

	partition, ok := partitionMetadataStruct.Get("partition").(int32)
	a.True(ok)
	a.Equal(int32(1), partition)

	leader, ok := partitionMetadataStruct.Get("leader").(int32)
	a.True(ok)
	a.Equal(int32(7), leader)

	replicas, ok := partitionMetadataStruct.Get("replicas").([]interface{})
	a.True(ok)
	a.Len(replicas, 3)

	a.Equal(int32(1), replicas[0])
	a.Equal(int32(2), replicas[1])
	a.Equal(int32(3), replicas[2])

	isr, ok := partitionMetadataStruct.Get("isr").([]interface{})
	a.True(ok)
	a.Len(isr, 0)

	// topic_metadata[1]
	topicMetadataStruct, ok = topicMetadata[1].(*Struct)
	a.True(ok)
	a.Len(topicMetadataStruct.values, 3)

	errorCode, ok = topicMetadataStruct.Get("error_code").(int16)
	a.True(ok)
	a.Equal(int16(0), errorCode)

	topic, ok = topicMetadataStruct.Get("topic").(string)
	a.True(ok)
	a.Equal("bar", topic)

	partitionMetadata, ok = topicMetadataStruct.Get("partition_metadata").([]interface{})
	a.True(ok)
	a.Len(partitionMetadata, 0)

	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)
}

func TestEmptyMetadataResponseV0(t *testing.T) {
	a := assert.New(t)

	schema := metadataResponseSchemaVersions[0]
	bytes := emptyMetadataResponse

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	dc := NewDecodeCheck()
	dc.Traverse(s)
	expected := []string{
		"[brokers]",
		"[topic_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)
}

func TestMetadataResponseV0(t *testing.T) {
	/*
	   Metadata Response (Version: 0) => [brokers] [topic_metadata]
	     brokers => node_id host port
	       node_id => INT32
	       host => STRING
	       port => INT32
	     topic_metadata => error_code topic [partition_metadata]
	       error_code => INT16
	       topic => STRING
	       partition_metadata => error_code partition leader [replicas] [isr]
	         error_code => INT16
	         partition => INT32
	         leader => INT32
	         replicas => INT32
	         isr => INT32
	*/

	bytes := []byte{
		// brokers
		0x00, 0x00, 0x00, 0x02,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273

		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x00,

		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		// partition_metadata
		0x00, 0x00, 0x00, 0x00}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[0]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	dc := NewDecodeCheck()
	dc.Traverse(s)
	expected := []string{
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)
}

type decodeCheck struct {
	attrValues []string
}

func NewDecodeCheck() *decodeCheck {
	return &decodeCheck{attrValues: make([]string, 0)}
}

func (t *decodeCheck) Traverse(s *Struct) error {
	for i, _ := range s.schema.fields {
		arg := s.values[i]
		if err := t.value(s, arg, i); err != nil {
			return err
		}
	}
	return nil
}

func (t *decodeCheck) value(s *Struct, arg interface{}, sindex int) error {
	name := s.schema.fields[sindex].def.GetName()

	switch v := arg.(type) {
	case bool:
		t.append(name, "bool", v)
	case int16:
		t.append(name, "int16", v)
	case int32:
		t.append(name, "int32", v)
	case string:
		t.append(name, "string", v)
	case *string:
		t.append(name, "*string", v)
	case *Struct:
		t.append(name, "struct")
		t.Traverse(v)
	case []interface{}:
		t.append(fmt.Sprintf("[%s]", name))
		for _, v := range v {
			if err := t.value(s, v, sindex); err != nil {
				return nil
			}
		}
	default:
		return fmt.Errorf("unknow type for value %v", arg)
	}
	return nil
}

func (t *decodeCheck) append(vs ...interface{}) {
	ss := make([]string, 0)
	for _, v := range vs {
		ss = append(ss, fmt.Sprint(v))
	}
	t.attrValues = append(t.attrValues, strings.Join(ss, " "))
}

func (t *decodeCheck) AttrValues() []string {
	return t.attrValues
}
func (t *decodeCheck) Dump() {
	for _, v := range t.attrValues {
		fmt.Printf("\"%s\",\n", v)
	}
}
