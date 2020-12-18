package protocol

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/grepplabs/kafka-proxy/config"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var (
	emptyMetadataResponse = []byte{
		// brokers
		0x00, 0x00, 0x00, 0x00,
		// topic_metadata
		0x00, 0x00, 0x00, 0x00}

	testResponseModifier = func(brokerHost string, brokerPort int32) (listenerHost string, listenerPort int32, err error) {
		if brokerHost == "localhost" && brokerPort == 51 {
			return "myhost1", 34001, nil
		} else if brokerHost == "google.com" && brokerPort == 273 {
			return "myhost2", 34002, nil
		} else if brokerHost == "kafka.org" && brokerPort == 53503 {
			return "myhost3", 34003, nil
		}
		return "", 0, errors.New("unexpected data")
	}

	testResponseModifier2 = func(brokerHost string, brokerPort int32) (listenerHost string, listenerPort int32, err error) {
		if brokerHost == "localhost" && brokerPort == 19092 {
			return "myhost1", 34001, nil
		} else if brokerHost == "localhost" && brokerPort == 29092 {
			return "myhost2", 34002, nil
		} else if brokerHost == "localhost" && brokerPort == 39092 {
			return "myhost3", 34003, nil
		} else if brokerHost == "localhost" && brokerPort == 9999 {
			return "myhost", 34000, nil
		}
		return "", 0, errors.New("unexpected data")
	}
)

func TestDecodeEmptyMetadataResponseV0(t *testing.T) {
	a := assert.New(t)

	schema := metadataResponseSchemaVersions[0]
	bytes := emptyMetadataResponse

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	a.Equal(`metadata_response_v0{brokers:[],topic_metadata:[]}`, s.String())

	a.Len(s.Values, 2)

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
	a.Len(s.Values, 2)

	brokers, ok := s.Get("brokers").([]interface{})
	a.True(ok)
	a.Len(brokers, 2)

	// broker[0]
	brokerStruct, ok := brokers[0].(*Struct)
	a.True(ok)
	a.Len(brokerStruct.Values, 3)

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
	a.Len(brokerStruct.Values, 3)

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
	a.Len(s.Values, 2)

	brokers, ok := s.Get("brokers").([]interface{})
	a.True(ok)
	a.Len(brokers, 0)

	topicMetadata, ok := s.Get("topic_metadata").([]interface{})
	a.True(ok)
	a.Len(topicMetadata, 2)

	// topic_metadata[0]
	topicMetadataStruct, ok := topicMetadata[0].(*Struct)
	a.True(ok)
	a.Len(topicMetadataStruct.Values, 3)

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
	a.Len(partitionMetadataStruct.Values, 5)

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
	a.Len(topicMetadataStruct.Values, 3)

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
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}
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

	apiVersion := int16(0)

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

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	dc := NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}
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

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, func(brokerHost string, brokerPort int32) (listenerHost string, listenerPort int32, err error) {
		if brokerHost == "localhost" && brokerPort == 51 {
			return "azure.microsoft.com", 34001, nil
		} else if brokerHost == "google.com" && brokerPort == 273 {
			return "aws.com", 34999, nil
		}
		return "", 0, errors.New("unexpected data")
	})
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}
	expected = []string{
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string azure.microsoft.com", // replaced
		"port int32 34001",                // replaced
		"brokers struct",
		"node_id int32 66051",
		"host string aws.com", // replaced
		"port int32 34999",    // replaced
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
}

func TestMetadataResponseV1(t *testing.T) {
	/*
	   Metadata Response (Version: 1) => [brokers] controller_id [topic_metadata]
	     brokers => node_id host port rack
	       node_id => INT32
	       host => STRING
	       port => INT32
	       rack => NULLABLE_STRING
	     controller_id => INT32
	     topic_metadata => error_code topic is_internal [partition_metadata]
	       error_code => INT16
	       topic => STRING
	       is_internal => BOOLEAN
	       partition_metadata => error_code partition leader [replicas] [isr]
	         error_code => INT16
	         partition => INT32
	         leader => INT32
	         replicas => INT32
	         isr => INT32
	*/

	apiVersion := int16(1)

	bytes := []byte{
		// brokers
		0x00, 0x00, 0x00, 0x03,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		0x00, 0x00, // ''
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273
		0x00, 0x07, 'e', 'u', 'w', 'e', 's', 't', '1',
		// brokers[2]
		0x00, 0x00, 0x00, 0x02, // 2
		0x00, 0x09, 'k', 'a', 'f', 'k', 'a', '.', 'o', 'r', 'g',
		0x00, 0x00, 0xd0, 0xff, // 53503
		0xff, 0xff, //  -1 is nil'

		// controller_id
		0x00, 0x00, 0xe1, 0xb2, // 57778

		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x01, // true
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02,

		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x00, // false
		// partition_metadata
		0x00, 0x00, 0x00, 0x00}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string kafka.org",
		"port int32 53503",
		"rack *string <nil>",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
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
		"isr int32 3",
		"isr int32 2",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}
	expected = []string{
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
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
		"isr int32 3",
		"isr int32 2",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponseV2(t *testing.T) {
	/*

	   Metadata Response (Version: 2) => [brokers] cluster_id controller_id [topic_metadata]
	     brokers => node_id host port rack
	       node_id => INT32
	       host => STRING
	       port => INT32
	       rack => NULLABLE_STRING
	     cluster_id => NULLABLE_STRING
	     controller_id => INT32
	     topic_metadata => error_code topic is_internal [partition_metadata]
	       error_code => INT16
	       topic => STRING
	       is_internal => BOOLEAN
	       partition_metadata => error_code partition leader [replicas] [isr]
	         error_code => INT16
	         partition => INT32
	         leader => INT32
	         replicas => INT32
	         isr => INT32
	*/

	apiVersion := int16(2)

	bytes := []byte{
		// brokers
		0x00, 0x00, 0x00, 0x03,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		0x00, 0x00, // ''
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273
		0x00, 0x07, 'e', 'u', 'w', 'e', 's', 't', '1',
		// brokers[2]
		0x00, 0x00, 0x00, 0x02, // 2
		0x00, 0x09, 'k', 'a', 'f', 'k', 'a', '.', 'o', 'r', 'g',
		0x00, 0x00, 0xd0, 0xff, // 53503
		0xff, 0xff, //  -1 is nil'

		// cluster_id
		0xff, 0xff, // nil

		// controller_id
		0x00, 0x00, 0xe1, 0xb2, // 57778

		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x01, // true
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02,

		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x00, // false
		// partition_metadata
		0x00, 0x00, 0x00, 0x00}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string kafka.org",
		"port int32 53503",
		"rack *string <nil>",
		"cluster_id *string <nil>",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
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
		"isr int32 3",
		"isr int32 2",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}
	expected = []string{
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"cluster_id *string <nil>",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
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
		"isr int32 3",
		"isr int32 2",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponseV3(t *testing.T) {
	/*
	   Metadata Response (Version: 3) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
	     throttle_time_ms => INT32
	     brokers => node_id host port rack
	       node_id => INT32
	       host => STRING
	       port => INT32
	       rack => NULLABLE_STRING
	     cluster_id => NULLABLE_STRING
	     controller_id => INT32
	     topic_metadata => error_code topic is_internal [partition_metadata]
	       error_code => INT16
	       topic => STRING
	       is_internal => BOOLEAN
	       partition_metadata => error_code partition leader [replicas] [isr]
	         error_code => INT16
	         partition => INT32
	         leader => INT32
	         replicas => INT32
	         isr => INT32

	*/

	apiVersion := int16(3)

	bytes := []byte{
		// throttle_time_ms
		0x00, 0x00, 0x00, 0x01, // 1
		// brokers
		0x00, 0x00, 0x00, 0x03,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		0x00, 0x00, // ''
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273
		0x00, 0x07, 'e', 'u', 'w', 'e', 's', 't', '1',
		// brokers[2]
		0x00, 0x00, 0x00, 0x02, // 2
		0x00, 0x09, 'k', 'a', 'f', 'k', 'a', '.', 'o', 'r', 'g',
		0x00, 0x00, 0xd0, 0xff, // 53503
		0xff, 0xff, //  -1 is nil'

		// cluster_id
		0x00, 0x07, 'm', 'y', 'k', 'a', 'f', 'k', 'a',

		// controller_id
		0x00, 0x00, 0xe1, 0xb2, // 57778

		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x01, // true
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02,

		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x00, // false
		// partition_metadata
		0x00, 0x00, 0x00, 0x00}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string kafka.org",
		"port int32 53503",
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
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
		"isr int32 3",
		"isr int32 2",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}
	expected = []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
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
		"isr int32 3",
		"isr int32 2",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponseV4(t *testing.T) {
	// SAME as V3
	/*
	   Metadata Response (Version: 4) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
	     throttle_time_ms => INT32
	     brokers => node_id host port rack
	       node_id => INT32
	       host => STRING
	       port => INT32
	       rack => NULLABLE_STRING
	     cluster_id => NULLABLE_STRING
	     controller_id => INT32
	     topic_metadata => error_code topic is_internal [partition_metadata]
	       error_code => INT16
	       topic => STRING
	       is_internal => BOOLEAN
	       partition_metadata => error_code partition leader [replicas] [isr]
	         error_code => INT16
	         partition => INT32
	         leader => INT32
	         replicas => INT32
	         isr => INT32
	*/

	apiVersion := int16(4)

	bytes := []byte{
		// throttle_time_ms
		0x00, 0x00, 0x00, 0x01, // 1
		// brokers
		0x00, 0x00, 0x00, 0x03,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		0x00, 0x00, // ''
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273
		0x00, 0x07, 'e', 'u', 'w', 'e', 's', 't', '1',
		// brokers[2]
		0x00, 0x00, 0x00, 0x02, // 2
		0x00, 0x09, 'k', 'a', 'f', 'k', 'a', '.', 'o', 'r', 'g',
		0x00, 0x00, 0xd0, 0xff, // 53503
		0xff, 0xff, //  -1 is nil'

		// cluster_id
		0x00, 0x07, 'm', 'y', 'k', 'a', 'f', 'k', 'a',

		// controller_id
		0x00, 0x00, 0xe1, 0xb2, // 57778

		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x01, // true
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02,

		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x00, // false
		// partition_metadata
		0x00, 0x00, 0x00, 0x00}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string kafka.org",
		"port int32 53503",
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
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
		"isr int32 3",
		"isr int32 2",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}
	expected = []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
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
		"isr int32 3",
		"isr int32 2",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponseV5(t *testing.T) {
	/*
	   Metadata Response (Version: 5) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
	     throttle_time_ms => INT32
	     brokers => node_id host port rack
	       node_id => INT32
	       host => STRING
	       port => INT32
	       rack => NULLABLE_STRING
	     cluster_id => NULLABLE_STRING
	     controller_id => INT32
	     topic_metadata => error_code topic is_internal [partition_metadata]
	       error_code => INT16
	       topic => STRING
	       is_internal => BOOLEAN
	       partition_metadata => error_code partition leader [replicas] [isr] [offline_replicas]
	         error_code => INT16
	         partition => INT32
	         leader => INT32
	         replicas => INT32
	         isr => INT32
	         offline_replicas => INT32
	*/

	apiVersion := int16(5)

	bytes := []byte{
		// throttle_time_ms
		0x00, 0x00, 0x00, 0x01, // 1
		// brokers
		0x00, 0x00, 0x00, 0x03,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		0x00, 0x00, // ''
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273
		0x00, 0x07, 'e', 'u', 'w', 'e', 's', 't', '1',
		// brokers[2]
		0x00, 0x00, 0x00, 0x02, // 2
		0x00, 0x09, 'k', 'a', 'f', 'k', 'a', '.', 'o', 'r', 'g',
		0x00, 0x00, 0xd0, 0xff, // 53503
		0xff, 0xff, //  -1 is nil'

		// cluster_id
		0x00, 0x07, 'm', 'y', 'k', 'a', 'f', 'k', 'a',

		// controller_id
		0x00, 0x00, 0xe1, 0xb2, // 57778

		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x01, // true
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x07,
		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x00, // false
		// partition_metadata
		0x00, 0x00, 0x00, 0x00}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string kafka.org",
		"port int32 53503",
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
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
		"isr int32 3",
		"isr int32 2",
		"[offline_replicas]",
		"offline_replicas int32 5",
		"offline_replicas int32 6",
		"offline_replicas int32 7",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}
	expected = []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
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
		"isr int32 3",
		"isr int32 2",
		"[offline_replicas]",
		"offline_replicas int32 5",
		"offline_replicas int32 6",
		"offline_replicas int32 7",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponseV6(t *testing.T) {
	/*
		Metadata Response (Version: 6) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
		  throttle_time_ms => INT32
		  brokers => node_id host port rack
			node_id => INT32
			host => STRING
			port => INT32
			rack => NULLABLE_STRING
		  cluster_id => NULLABLE_STRING
		  controller_id => INT32
		  topic_metadata => error_code topic is_internal [partition_metadata]
			error_code => INT16
			topic => STRING
			is_internal => BOOLEAN
			partition_metadata => error_code partition leader [replicas] [isr] [offline_replicas]
			  error_code => INT16
			  partition => INT32
			  leader => INT32
			  replicas => INT32
			  isr => INT32
			  offline_replicas => INT32

	*/

	apiVersion := int16(6)

	bytes := []byte{
		// throttle_time_ms
		0x00, 0x00, 0x00, 0x01, // 1
		// brokers
		0x00, 0x00, 0x00, 0x03,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		0x00, 0x00, // ''
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273
		0x00, 0x07, 'e', 'u', 'w', 'e', 's', 't', '1',
		// brokers[2]
		0x00, 0x00, 0x00, 0x02, // 2
		0x00, 0x09, 'k', 'a', 'f', 'k', 'a', '.', 'o', 'r', 'g',
		0x00, 0x00, 0xd0, 0xff, // 53503
		0xff, 0xff, //  -1 is nil'

		// cluster_id
		0x00, 0x07, 'm', 'y', 'k', 'a', 'f', 'k', 'a',

		// controller_id
		0x00, 0x00, 0xe1, 0xb2, // 57778

		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x01, // true
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x07,
		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x00, // false
		// partition_metadata
		0x00, 0x00, 0x00, 0x00}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string kafka.org",
		"port int32 53503",
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
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
		"isr int32 3",
		"isr int32 2",
		"[offline_replicas]",
		"offline_replicas int32 5",
		"offline_replicas int32 6",
		"offline_replicas int32 7",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}
	expected = []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
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
		"isr int32 3",
		"isr int32 2",
		"[offline_replicas]",
		"offline_replicas int32 5",
		"offline_replicas int32 6",
		"offline_replicas int32 7",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponseV7(t *testing.T) {
	/*
		Metadata Response (Version: 7) => throttle_time_ms [brokers] cluster_id controller_id [topic_metadata]
		  throttle_time_ms => INT32
		  brokers => node_id host port rack
			node_id => INT32
			host => STRING
			port => INT32
			rack => NULLABLE_STRING
		  cluster_id => NULLABLE_STRING
		  controller_id => INT32
		  topic_metadata => error_code topic is_internal [partition_metadata]
			error_code => INT16
			topic => STRING
			is_internal => BOOLEAN
			partition_metadata => error_code partition leader leader_epoch [replicas] [isr] [offline_replicas]
			  error_code => INT16
			  partition => INT32
			  leader => INT32
			  leader_epoch => INT32
			  replicas => INT32
			  isr => INT32
			  offline_replicas => INT32

	*/

	apiVersion := int16(7)

	bytes := []byte{
		// throttle_time_ms
		0x00, 0x00, 0x00, 0x01, // 1
		// brokers
		0x00, 0x00, 0x00, 0x03,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		0x00, 0x00, // ''
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273
		0x00, 0x07, 'e', 'u', 'w', 'e', 's', 't', '1',
		// brokers[2]
		0x00, 0x00, 0x00, 0x02, // 2
		0x00, 0x09, 'k', 'a', 'f', 'k', 'a', '.', 'o', 'r', 'g',
		0x00, 0x00, 0xd0, 0xff, // 53503
		0xff, 0xff, //  -1 is nil'

		// cluster_id
		0x00, 0x07, 'm', 'y', 'k', 'a', 'f', 'k', 'a',

		// controller_id
		0x00, 0x00, 0xe1, 0xb2, // 57778

		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x01, // true
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x08,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x07,
		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x00, // false
		// partition_metadata
		0x00, 0x00, 0x00, 0x00}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string kafka.org",
		"port int32 53503",
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"leader_epoch int32 8",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"[offline_replicas]",
		"offline_replicas int32 5",
		"offline_replicas int32 6",
		"offline_replicas int32 7",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}
	expected = []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"leader_epoch int32 8",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"[offline_replicas]",
		"offline_replicas int32 5",
		"offline_replicas int32 6",
		"offline_replicas int32 7",
		"topic_metadata struct",
		"error_code int16 0",
		"topic string bar",
		"is_internal bool false",
		"[partition_metadata]",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponseV8(t *testing.T) {
	/*
		Metadata Response (Version: 8) => throttle_time_ms [brokers] cluster_id controller_id [topics] cluster_authorized_operations
		  throttle_time_ms => INT32
		  brokers => node_id host port rack
		    node_id => INT32
		    host => STRING
		    port => INT32
		    rack => NULLABLE_STRING
		  cluster_id => NULLABLE_STRING
		  controller_id => INT32
		  topics => error_code name is_internal [partitions] topic_authorized_operations
		    error_code => INT16
		    name => STRING
		    is_internal => BOOLEAN
		    partitions => error_code partition_index leader_id leader_epoch [replica_nodes] [isr_nodes] [offline_replicas]
		      error_code => INT16
		      partition_index => INT32
		      leader_id => INT32
		      leader_epoch => INT32
		      replica_nodes => INT32
		      isr_nodes => INT32
		      offline_replicas => INT32
		    topic_authorized_operations => INT32
		  cluster_authorized_operations => INT32
	*/

	apiVersion := int16(8)

	bytes := []byte{
		// throttle_time_ms
		0x00, 0x00, 0x00, 0x01, // 1
		// brokers
		0x00, 0x00, 0x00, 0x03,
		// brokers[0]
		0x00, 0x00, 0xab, 0xff, // 44031
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
		0x00, 0x00, // ''
		// brokers[1]
		0x00, 0x01, 0x02, 0x03, // 66051
		0x00, 0x0a, 'g', 'o', 'o', 'g', 'l', 'e', '.', 'c', 'o', 'm',
		0x00, 0x00, 0x01, 0x11, // 273
		0x00, 0x07, 'e', 'u', 'w', 'e', 's', 't', '1',
		// brokers[2]
		0x00, 0x00, 0x00, 0x02, // 2
		0x00, 0x09, 'k', 'a', 'f', 'k', 'a', '.', 'o', 'r', 'g',
		0x00, 0x00, 0xd0, 0xff, // 53503
		0xff, 0xff, //  -1 is nil'

		// cluster_id
		0x00, 0x07, 'm', 'y', 'k', 'a', 'f', 'k', 'a',

		// controller_id
		0x00, 0x00, 0xe1, 0xb2, // 57778

		// topic_metadata
		0x00, 0x00, 0x00, 0x02,

		// topic_metadata[0]
		0x00, 0x00,
		0x00, 0x03, 'f', 'o', 'o',
		0x01, // true
		// partition_metadata
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x04,
		0x00, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x08,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
		0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02,
		0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x06, 0x00, 0x00, 0x00, 0x07,
		0x00, 0x00, 0x00, 0x08, // topic_authorized_operations 8
		// topic_metadata[1]
		0x00, 0x00,
		0x00, 0x03, 'b', 'a', 'r',
		0x00, // false
		// partition_metadata
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x04, // topic_authorized_operations 4
		// cluster_authorized_operations 5
		0x00, 0x00, 0x00, 0x05,
	}

	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string localhost",
		"port int32 51",
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string google.com",
		"port int32 273",
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string kafka.org",
		"port int32 53503",
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"name string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"leader_epoch int32 8",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"[offline_replicas]",
		"offline_replicas int32 5",
		"offline_replicas int32 6",
		"offline_replicas int32 7",
		"topic_authorized_operations int32 8",
		"topic_metadata struct",
		"error_code int16 0",
		"name string bar",
		"is_internal bool false",
		"[partition_metadata]",
		"topic_authorized_operations int32 4",
		"cluster_authorized_operations int32 5",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}
	expected = []string{
		"throttle_time_ms int32 1",
		"[brokers]",
		"brokers struct",
		"node_id int32 44031",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string ",
		"brokers struct",
		"node_id int32 66051",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string euwest1",
		"brokers struct",
		"node_id int32 2",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"cluster_id *string mykafka",
		"controller_id int32 57778",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"name string foo",
		"is_internal bool true",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 4",
		"partition int32 1",
		"leader int32 7",
		"leader_epoch int32 8",
		"[replicas]",
		"replicas int32 1",
		"replicas int32 2",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"isr int32 2",
		"[offline_replicas]",
		"offline_replicas int32 5",
		"offline_replicas int32 6",
		"offline_replicas int32 7",
		"topic_authorized_operations int32 8",
		"topic_metadata struct",
		"error_code int16 0",
		"name string bar",
		"is_internal bool false",
		"[partition_metadata]",
		"topic_authorized_operations int32 4",
		"cluster_authorized_operations int32 5",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponseV9(t *testing.T) {
	apiVersion := int16(9)

	bytes, err := hex.DecodeString("0000000004000000020a6c6f63616c686f7374000071a40000000000030a6c6f63616c686f7374000098b40000000000010a6c6f63616c686f737400004a9400001763754b7373754b3052726d4950586164374259426b670000000202000010746573742d6e6f2d686561646572730002000000000000000000030000000002000000030200000003010000000000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	a := assert.New(t)

	schema := metadataResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)

	dc := NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"throttle_time_ms int32 0",
		"[brokers]",
		"brokers struct",
		"node_id int32 2",
		"host string localhost",
		"port int32 29092",
		"rack *string <nil>",
		"[broker_tagged_fields]",
		"brokers struct",
		"node_id int32 3",
		"host string localhost",
		"port int32 39092",
		"rack *string <nil>",
		"[broker_tagged_fields]",
		"brokers struct",
		"node_id int32 1",
		"host string localhost",
		"port int32 19092",
		"rack *string <nil>",
		"[broker_tagged_fields]",
		"cluster_id *string cuKssuK0RrmIPXad7BYBkg",
		"controller_id int32 2",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"name string test-no-headers",
		"is_internal bool false",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 0",
		"partition int32 0",
		"leader int32 3",
		"leader_epoch int32 0",
		"[replicas]",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"[offline_replicas]",
		"[partition_metadata_tagged_fields]",
		"topic_authorized_operations int32 0",
		"[topic_metadata_tagged_fields]",
		"cluster_authorized_operations int32 0",
		"[response_tagged_fields]",
	}

	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyMetadata, apiVersion, testResponseModifier2)
	if err != nil {
		t.Fatal(err)
	}
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}

	expected = []string{
		"throttle_time_ms int32 0",
		"[brokers]",
		"brokers struct",
		"node_id int32 2",
		"host string myhost2", // replaced
		"port int32 34002",    // replaced
		"rack *string <nil>",
		"[broker_tagged_fields]",
		"brokers struct",
		"node_id int32 3",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"rack *string <nil>",
		"[broker_tagged_fields]",
		"brokers struct",
		"node_id int32 1",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
		"rack *string <nil>",
		"[broker_tagged_fields]",
		"cluster_id *string cuKssuK0RrmIPXad7BYBkg",
		"controller_id int32 2",
		"[topic_metadata]",
		"topic_metadata struct",
		"error_code int16 0",
		"name string test-no-headers",
		"is_internal bool false",
		"[partition_metadata]",
		"partition_metadata struct",
		"error_code int16 0",
		"partition int32 0",
		"leader int32 3",
		"leader_epoch int32 0",
		"[replicas]",
		"replicas int32 3",
		"[isr]",
		"isr int32 3",
		"[offline_replicas]",
		"[partition_metadata_tagged_fields]",
		"topic_authorized_operations int32 0",
		"[topic_metadata_tagged_fields]",
		"cluster_authorized_operations int32 0",
		"[response_tagged_fields]",
	}

	a.Equal(expected, dc.AttrValues())
}

func TestFindCoordinatorResponseV0(t *testing.T) {
	/*
	   FindCoordinator Response (Version: 0) => error_code coordinator
	     error_code => INT16
	     coordinator => node_id host port
	       node_id => INT32
	       host => STRING
	       port => INT32
	*/
	apiVersion := int16(0)

	bytes := []byte{
		0x00, 0x00,
		// coordinator
		0x00, 0x00, 0x00, 0xAB,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
	}
	a := assert.New(t)

	schema := findCoordinatorResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	dc := NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"error_code int16 0",
		"coordinator struct",
		"node_id int32 171",
		"host string localhost",
		"port int32 51",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyFindCoordinator, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}
	expected = []string{
		"error_code int16 0",
		"coordinator struct",
		"node_id int32 171",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
	}
	a.Equal(expected, dc.AttrValues())
}

func TestFindCoordinatorResponseV1(t *testing.T) {
	/*
	   FindCoordinator Response (Version: 1) => throttle_time_ms error_code error_message coordinator
	     throttle_time_ms => INT32
	     error_code => INT16
	     error_message => NULLABLE_STRING
	     coordinator => node_id host port
	       node_id => INT32
	       host => STRING
	       port => INT32
	*/
	apiVersion := int16(1)

	bytes := []byte{
		// throttle_time_ms
		0x00, 0x00, 0x00, 0x01, // 1
		0x00, 0x00,
		0xff, 0xff,
		// coordinator
		0x00, 0x00, 0x00, 0xAB,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
	}
	a := assert.New(t)

	schema := findCoordinatorResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	dc := NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"throttle_time_ms int32 1",
		"error_code int16 0",
		"error_message *string <nil>",
		"coordinator struct",
		"node_id int32 171",
		"host string localhost",
		"port int32 51",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyFindCoordinator, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}
	expected = []string{
		"throttle_time_ms int32 1",
		"error_code int16 0",
		"error_message *string <nil>",
		"coordinator struct",
		"node_id int32 171",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
	}
	a.Equal(expected, dc.AttrValues())
}

func TestFindCoordinatorResponseV2(t *testing.T) {
	/*
	   FindCoordinator Response (Version: 2) => throttle_time_ms error_code error_message coordinator
	     throttle_time_ms => INT32
	     error_code => INT16
	     error_message => NULLABLE_STRING
	     coordinator => node_id host port
	       node_id => INT32
	       host => STRING
	       port => INT32
	*/
	apiVersion := int16(2)

	bytes := []byte{
		// throttle_time_ms
		0x00, 0x00, 0x00, 0x01, // 1
		0x00, 0x00,
		0xff, 0xff,
		// coordinator
		0x00, 0x00, 0x00, 0xAB,
		0x00, 0x09, 'l', 'o', 'c', 'a', 'l', 'h', 'o', 's', 't',
		0x00, 0x00, 0x00, 0x33, // 51
	}
	a := assert.New(t)

	schema := findCoordinatorResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	dc := NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"throttle_time_ms int32 1",
		"error_code int16 0",
		"error_message *string <nil>",
		"coordinator struct",
		"node_id int32 171",
		"host string localhost",
		"port int32 51",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyFindCoordinator, apiVersion, testResponseModifier)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}
	expected = []string{
		"throttle_time_ms int32 1",
		"error_code int16 0",
		"error_message *string <nil>",
		"coordinator struct",
		"node_id int32 171",
		"host string myhost1", // replaced
		"port int32 34001",    // replaced
	}
	a.Equal(expected, dc.AttrValues())
}

func TestFindCoordinatorResponseV3(t *testing.T) {
	apiVersion := int16(3)

	// "00000000000f2254686520636f6f7264696e61746f72206973206e6f7420617661696c61626c652effffffff01ffffffff00"
	bytes, err := hex.DecodeString("000000000000054e4f4e45000000030a6c6f63616c686f7374000098b400")
	if err != nil {
		t.Fatal(err)
	}
	a := assert.New(t)

	schema := findCoordinatorResponseSchemaVersions[apiVersion]

	s, err := DecodeSchema(bytes, schema)
	a.Nil(err)
	dc := NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{
		"throttle_time_ms int32 0",
		"error_code int16 0",
		"error_message *string NONE",
		"coordinator struct",
		"node_id int32 3",
		"host string localhost",
		"port int32 39092",
		"[response_tagged_fields]",
	}
	a.Equal(expected, dc.AttrValues())
	resp, err := EncodeSchema(s, schema)
	a.Nil(err)
	a.Equal(bytes, resp)

	modifier, err := GetResponseModifier(apiKeyFindCoordinator, apiVersion, testResponseModifier2)
	a.Nil(err)
	resp, err = modifier.Apply(resp)
	a.Nil(err)
	s, err = DecodeSchema(resp, schema)
	a.Nil(err)
	dc = NewDecodeCheck()
	err = dc.Traverse(s)
	if err != nil {
		t.Fatal(err)
	}
	expected = []string{
		"throttle_time_ms int32 0",
		"error_code int16 0",
		"error_message *string NONE",
		"coordinator struct",
		"node_id int32 3",
		"host string myhost3", // replaced
		"port int32 34003",    // replaced
		"[response_tagged_fields]",
	}
	a.Equal(expected, dc.AttrValues())
}

func TestMetadataResponses(t *testing.T) {
	tt := []struct {
		name       string
		apiVersion int16
		hexInput   string
		expected   []string
		modifier   config.NetAddressMappingFunc
		modified   []string
	}{
		{name: "v0", apiVersion: int16(0),
			hexInput: "000000010000000000096c6f63616c686f73740000270f00000001000800125f5f636f6e73756d65725f6f66667365747300000001ffff000000000000000b00000001000000010000000100000002",
			expected: []string{"[brokers]", "brokers struct", "node_id int32 0", "host string localhost", "port int32 9999", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "topic string __consumer_offsets", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2"},
			modifier: testResponseModifier2,
			modified: []string{"[brokers]", "brokers struct", "node_id int32 0", "host string myhost", "port int32 34000", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "topic string __consumer_offsets", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2"},
		},
		{name: "v1", apiVersion: int16(1),
			hexInput: "000000010000000000096c6f63616c686f73740000270f00087261636b2d312d310000000000000001000800125f5f636f6e73756d65725f6f6666736574730100000001ffff000000000000000b00000001000000010000000100000002",
			expected: []string{"[brokers]", "brokers struct", "node_id int32 0", "host string localhost", "port int32 9999", "rack *string rack-1-1", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "topic string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2"},
			modifier: testResponseModifier2,
			modified: []string{"[brokers]", "brokers struct", "node_id int32 0", "host string myhost", "port int32 34000", "rack *string rack-1-1", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "topic string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2"},
		},
		{name: "v2", apiVersion: int16(2),
			hexInput: "000000010000000000096c6f63616c686f73740000270f00087261636b2d312d3100096d79636c75737465720000000000000001000800125f5f636f6e73756d65725f6f6666736574730100000001ffff000000000000000b00000001000000010000000100000002",
			expected: []string{"[brokers]", "brokers struct", "node_id int32 0", "host string localhost", "port int32 9999", "rack *string rack-1-1", "cluster_id *string mycluster", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "topic string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2"},
			modifier: testResponseModifier2,
			modified: []string{"[brokers]", "brokers struct", "node_id int32 0", "host string myhost", "port int32 34000", "rack *string rack-1-1", "cluster_id *string mycluster", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "topic string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2"},
		},
		{name: "v3", apiVersion: int16(3),
			hexInput: "00000000000000010000000000096c6f63616c686f73740000270f00087261636b2d312d3100096d79636c75737465720000000000000001000800125f5f636f6e73756d65725f6f6666736574730100000001ffff000000000000000b00000001000000010000000100000002",
			expected: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string localhost", "port int32 9999", "rack *string rack-1-1", "cluster_id *string mycluster", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "topic string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string myhost", "port int32 34000", "rack *string rack-1-1", "cluster_id *string mycluster", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "topic string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2"},
		},
		{name: "v4", apiVersion: int16(4),
			hexInput: "00000000000000010000000000096c6f63616c686f73740000270f00087261636b2d312d3100096d79636c75737465720000000000000001000800125f5f636f6e73756d65725f6f6666736574730100000001ffff000000000000000b00000001000000010000000100000002",
			expected: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string localhost", "port int32 9999", "rack *string rack-1-1", "cluster_id *string mycluster", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "topic string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string myhost", "port int32 34000", "rack *string rack-1-1", "cluster_id *string mycluster", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "topic string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2"},
		},
		{name: "v5", apiVersion: int16(5),
			hexInput: "00000000000000010000000000096c6f63616c686f73740000270f00087261636b2d312d3100096d79636c75737465720000000000000001000800125f5f636f6e73756d65725f6f6666736574730100000001ffff000000000000000b000000010000000100000001000000020000000100000003",
			expected: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string localhost", "port int32 9999", "rack *string rack-1-1", "cluster_id *string mycluster", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "topic string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string myhost", "port int32 34000", "rack *string rack-1-1", "cluster_id *string mycluster", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "topic string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3"},
		},
		{name: "v6", apiVersion: int16(6),
			hexInput: "00000000000000010000000000096c6f63616c686f73740000270f00087261636b2d312d3100096d79636c75737465720000000000000001000800125f5f636f6e73756d65725f6f6666736574730100000001ffff000000000000000b000000010000000100000001000000020000000100000003",
			expected: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string localhost", "port int32 9999", "rack *string rack-1-1", "cluster_id *string mycluster", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "topic string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string myhost", "port int32 34000", "rack *string rack-1-1", "cluster_id *string mycluster", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "topic string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3"},
		},
		{name: "v7", apiVersion: int16(7),
			hexInput: "00000000000000010000000000096c6f63616c686f73740000270f00087261636b2d312d3100096d79636c75737465720000000000000001000800125f5f636f6e73756d65725f6f6666736574730100000001ffff000000000000000b0000000c000000010000000100000001000000020000000100000003",
			expected: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string localhost", "port int32 9999", "rack *string rack-1-1", "cluster_id *string mycluster", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "topic string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "leader_epoch int32 12", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string myhost", "port int32 34000", "rack *string rack-1-1", "cluster_id *string mycluster", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "topic string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "leader_epoch int32 12", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3"},
		},
		{name: "v8", apiVersion: int16(8),
			hexInput: "00000000000000010000000000096c6f63616c686f73740000270f00087261636b2d312d3100096d79636c75737465720000000000000001000800125f5f636f6e73756d65725f6f6666736574730100000001ffff000000000000000b0000000c0000000100000001000000010000000200000001000000038000000080000000",
			expected: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string localhost", "port int32 9999", "rack *string rack-1-1", "cluster_id *string mycluster", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "name string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "leader_epoch int32 12", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3", "topic_authorized_operations int32 -2147483648", "cluster_authorized_operations int32 -2147483648"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string myhost", "port int32 34000", "rack *string rack-1-1", "cluster_id *string mycluster", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "name string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "leader_epoch int32 12", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3", "topic_authorized_operations int32 -2147483648", "cluster_authorized_operations int32 -2147483648"},
		},
		{name: "v9 1", apiVersion: int16(9),
			hexInput: "0000000002000000000a6c6f63616c686f73740000270f097261636b2d312d31000a6d79636c757374657200000000020008135f5f636f6e73756d65725f6f6666736574730102ffff000000000000000b0000000c0200000001020000000202000000030000004e22000000271100",
			expected: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string localhost", "port int32 9999", "rack *string rack-1-1", "[broker_tagged_fields]", "cluster_id *string mycluster", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "name string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "leader_epoch int32 12", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3", "[partition_metadata_tagged_fields]", "topic_authorized_operations int32 20002", "[topic_metadata_tagged_fields]", "cluster_authorized_operations int32 10001", "[response_tagged_fields]"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string myhost", "port int32 34000", "rack *string rack-1-1", "[broker_tagged_fields]", "cluster_id *string mycluster", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 8", "name string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 -1", "partition int32 0", "leader int32 11", "leader_epoch int32 12", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3", "[partition_metadata_tagged_fields]", "topic_authorized_operations int32 20002", "[topic_metadata_tagged_fields]", "cluster_authorized_operations int32 10001", "[response_tagged_fields]"},
		},
		{name: "v9 2", apiVersion: int16(9),
			hexInput: "0000000002000000000a6c6f63616c686f73740000270f00000a636c7573746572496400000000020000135f5f636f6e73756d65725f6f6666736574730102000000000000ffffffffffffffff0200000001020000000202000000030080000000008000000000",
			expected: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string localhost", "port int32 9999", "rack *string <nil>", "[broker_tagged_fields]", "cluster_id *string clusterId", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 0", "name string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 0", "partition int32 0", "leader int32 -1", "leader_epoch int32 -1", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3", "[partition_metadata_tagged_fields]", "topic_authorized_operations int32 -2147483648", "[topic_metadata_tagged_fields]", "cluster_authorized_operations int32 -2147483648", "[response_tagged_fields]"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string myhost", "port int32 34000", "rack *string <nil>", "[broker_tagged_fields]", "cluster_id *string clusterId", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 0", "name string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 0", "partition int32 0", "leader int32 -1", "leader_epoch int32 -1", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3", "[partition_metadata_tagged_fields]", "topic_authorized_operations int32 -2147483648", "[topic_metadata_tagged_fields]", "cluster_authorized_operations int32 -2147483648", "[response_tagged_fields]"},
		},
		{name: "v9, response_tagged_fields", apiVersion: int16(9),
			hexInput: "0000000002000000000a6c6f63616c686f73740000270f00000a636c7573746572496400000000020000135f5f636f6e73756d65725f6f6666736574730102000000000000ffffffffffffffff0200000001020000000202000000030080000000008000000001000101",
			expected: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string localhost", "port int32 9999", "rack *string <nil>", "[broker_tagged_fields]", "cluster_id *string clusterId", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 0", "name string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 0", "partition int32 0", "leader int32 -1", "leader_epoch int32 -1", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3", "[partition_metadata_tagged_fields]", "topic_authorized_operations int32 -2147483648", "[topic_metadata_tagged_fields]", "cluster_authorized_operations int32 -2147483648", "[response_tagged_fields]", "response_tagged_fields tag 0 value 0x01"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string myhost", "port int32 34000", "rack *string <nil>", "[broker_tagged_fields]", "cluster_id *string clusterId", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 0", "name string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 0", "partition int32 0", "leader int32 -1", "leader_epoch int32 -1", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3", "[partition_metadata_tagged_fields]", "topic_authorized_operations int32 -2147483648", "[topic_metadata_tagged_fields]", "cluster_authorized_operations int32 -2147483648", "[response_tagged_fields]", "response_tagged_fields tag 0 value 0x01"},
		},
		{name: "v9, 2 response_tagged_fields", apiVersion: int16(9),
			hexInput: "0000000002000000000a6c6f63616c686f73740000270f00000a636c7573746572496400000000020000135f5f636f6e73756d65725f6f6666736574730102000000000000ffffffffffffffff020000000102000000020200000003008000000000800000000200010101020203",
			expected: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string localhost", "port int32 9999", "rack *string <nil>", "[broker_tagged_fields]", "cluster_id *string clusterId", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 0", "name string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 0", "partition int32 0", "leader int32 -1", "leader_epoch int32 -1", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3", "[partition_metadata_tagged_fields]", "topic_authorized_operations int32 -2147483648", "[topic_metadata_tagged_fields]", "cluster_authorized_operations int32 -2147483648", "[response_tagged_fields]", "response_tagged_fields tag 0 value 0x01", "response_tagged_fields tag 1 value 0x0203"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string myhost", "port int32 34000", "rack *string <nil>", "[broker_tagged_fields]", "cluster_id *string clusterId", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 0", "name string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 0", "partition int32 0", "leader int32 -1", "leader_epoch int32 -1", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3", "[partition_metadata_tagged_fields]", "topic_authorized_operations int32 -2147483648", "[topic_metadata_tagged_fields]", "cluster_authorized_operations int32 -2147483648", "[response_tagged_fields]", "response_tagged_fields tag 0 value 0x01", "response_tagged_fields tag 1 value 0x0203"},
		},
		{name: "v9, topic_metadata_tagged_fields", apiVersion: int16(9),
			hexInput: "0000000002000000000a6c6f63616c686f73740000270f00000a636c7573746572496400000000020000135f5f636f6e73756d65725f6f6666736574730102000000000000ffffffffffffffff020000000102000000020200000003008000000001000247118000000000",
			expected: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string localhost", "port int32 9999", "rack *string <nil>", "[broker_tagged_fields]", "cluster_id *string clusterId", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 0", "name string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 0", "partition int32 0", "leader int32 -1", "leader_epoch int32 -1", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3", "[partition_metadata_tagged_fields]", "topic_authorized_operations int32 -2147483648", "[topic_metadata_tagged_fields]", "topic_metadata_tagged_fields tag 0 value 0x4711", "cluster_authorized_operations int32 -2147483648", "[response_tagged_fields]"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string myhost", "port int32 34000", "rack *string <nil>", "[broker_tagged_fields]", "cluster_id *string clusterId", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 0", "name string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 0", "partition int32 0", "leader int32 -1", "leader_epoch int32 -1", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3", "[partition_metadata_tagged_fields]", "topic_authorized_operations int32 -2147483648", "[topic_metadata_tagged_fields]", "topic_metadata_tagged_fields tag 0 value 0x4711", "cluster_authorized_operations int32 -2147483648", "[response_tagged_fields]"},
		},
		{name: "v9, partition_metadata_tagged_fields", apiVersion: int16(9),
			hexInput: "0000000002000000000a6c6f63616c686f73740000270f00000a636c7573746572496400000000020000135f5f636f6e73756d65725f6f6666736574730102000000000000ffffffffffffffff020000000102000000020200000003010002471180000000008000000000",
			expected: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string localhost", "port int32 9999", "rack *string <nil>", "[broker_tagged_fields]", "cluster_id *string clusterId", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 0", "name string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 0", "partition int32 0", "leader int32 -1", "leader_epoch int32 -1", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3", "[partition_metadata_tagged_fields]", "partition_metadata_tagged_fields tag 0 value 0x4711", "topic_authorized_operations int32 -2147483648", "[topic_metadata_tagged_fields]", "cluster_authorized_operations int32 -2147483648", "[response_tagged_fields]"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string myhost", "port int32 34000", "rack *string <nil>", "[broker_tagged_fields]", "cluster_id *string clusterId", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 0", "name string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 0", "partition int32 0", "leader int32 -1", "leader_epoch int32 -1", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3", "[partition_metadata_tagged_fields]", "partition_metadata_tagged_fields tag 0 value 0x4711", "topic_authorized_operations int32 -2147483648", "[topic_metadata_tagged_fields]", "cluster_authorized_operations int32 -2147483648", "[response_tagged_fields]"},
		},
		{name: "v9, broker_tagged_fields", apiVersion: int16(9),
			hexInput: "0000000002000000000a6c6f63616c686f73740000270f0001000247110a636c7573746572496400000000020000135f5f636f6e73756d65725f6f6666736574730102000000000000ffffffffffffffff0200000001020000000202000000030080000000008000000000",
			expected: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string localhost", "port int32 9999", "rack *string <nil>", "[broker_tagged_fields]", "broker_tagged_fields tag 0 value 0x4711", "cluster_id *string clusterId", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 0", "name string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 0", "partition int32 0", "leader int32 -1", "leader_epoch int32 -1", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3", "[partition_metadata_tagged_fields]", "topic_authorized_operations int32 -2147483648", "[topic_metadata_tagged_fields]", "cluster_authorized_operations int32 -2147483648", "[response_tagged_fields]"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "[brokers]", "brokers struct", "node_id int32 0", "host string myhost", "port int32 34000", "rack *string <nil>", "[broker_tagged_fields]", "broker_tagged_fields tag 0 value 0x4711", "cluster_id *string clusterId", "controller_id int32 0", "[topic_metadata]", "topic_metadata struct", "error_code int16 0", "name string __consumer_offsets", "is_internal bool true", "[partition_metadata]", "partition_metadata struct", "error_code int16 0", "partition int32 0", "leader int32 -1", "leader_epoch int32 -1", "[replicas]", "replicas int32 1", "[isr]", "isr int32 2", "[offline_replicas]", "offline_replicas int32 3", "[partition_metadata_tagged_fields]", "topic_authorized_operations int32 -2147483648", "[topic_metadata_tagged_fields]", "cluster_authorized_operations int32 -2147483648", "[response_tagged_fields]"},
		},
	}
	for _, tc := range tt {
		bytes, err := hex.DecodeString(tc.hexInput)
		if err != nil {
			t.Fatal(err)
		}
		schema := metadataResponseSchemaVersions[tc.apiVersion]

		a := assert.New(t)
		s, err := DecodeSchema(bytes, schema)
		if err != nil {
			t.Fatal(err)
		}
		dc := NewDecodeCheck()
		err = dc.Traverse(s)
		if err != nil {
			t.Fatal(err)
		}
		a.Equal(tc.expected, dc.AttrValues(), "decode:"+tc.name)

		// encode
		resp, err := EncodeSchema(s, schema)
		if err != nil {
			t.Fatal(err)
		}
		a.Equal(bytes, resp, "encode:"+tc.name)

		// modify
		modifier, err := GetResponseModifier(apiKeyMetadata, tc.apiVersion, tc.modifier)
		if err != nil {
			t.Fatal(err)
		}
		resp, err = modifier.Apply(resp)
		if err != nil {
			t.Fatal(err)
		}
		s, err = DecodeSchema(resp, schema)
		if err != nil {
			t.Fatal(err)
		}
		dc = NewDecodeCheck()
		err = dc.Traverse(s)
		if err != nil {
			t.Fatal(err)
		}
		a.Equal(tc.modified, dc.AttrValues(), "modify:"+tc.name)
	}
}

func TestFindCoordinatorResponse(t *testing.T) {
	tt := []struct {
		name       string
		apiVersion int16
		hexInput   string
		expected   []string
		modifier   config.NetAddressMappingFunc
		modified   []string
	}{
		{name: "v0", apiVersion: int16(0),
			hexInput: "00000000000000096c6f63616c686f73740000270f",
			expected: []string{"error_code int16 0", "coordinator struct", "node_id int32 0", "host string localhost", "port int32 9999"},
			modifier: testResponseModifier2,
			modified: []string{"error_code int16 0", "coordinator struct", "node_id int32 0", "host string myhost", "port int32 34000"},
		},
		{name: "v1", apiVersion: int16(1),
			hexInput: "00000000000000044e4f4e450000000000096c6f63616c686f73740000270f",
			expected: []string{"throttle_time_ms int32 0", "error_code int16 0", "error_message *string NONE", "coordinator struct", "node_id int32 0", "host string localhost", "port int32 9999"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "error_code int16 0", "error_message *string NONE", "coordinator struct", "node_id int32 0", "host string myhost", "port int32 34000"},
		},
		{name: "v2", apiVersion: int16(2),
			hexInput: "00000000000000044e4f4e450000000000096c6f63616c686f73740000270f",
			expected: []string{"throttle_time_ms int32 0", "error_code int16 0", "error_message *string NONE", "coordinator struct", "node_id int32 0", "host string localhost", "port int32 9999"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "error_code int16 0", "error_message *string NONE", "coordinator struct", "node_id int32 0", "host string myhost", "port int32 34000"},
		},
		{name: "v2, with error", apiVersion: int16(2),
			hexInput: "000000000008001c5468652062726f6b6572206973206e6f7420617661696c61626c652e0000000000096c6f63616c686f73740000270f",
			expected: []string{"throttle_time_ms int32 0", "error_code int16 8", "error_message *string The broker is not available.", "coordinator struct", "node_id int32 0", "host string localhost", "port int32 9999"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "error_code int16 8", "error_message *string The broker is not available.", "coordinator struct", "node_id int32 0", "host string myhost", "port int32 34000"},
		},
		{name: "v3", apiVersion: int16(3),
			hexInput: "000000000000054e4f4e45000000000a6c6f63616c686f73740000270f00",
			expected: []string{"throttle_time_ms int32 0", "error_code int16 0", "error_message *string NONE", "coordinator struct", "node_id int32 0", "host string localhost", "port int32 9999", "[response_tagged_fields]"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "error_code int16 0", "error_message *string NONE", "coordinator struct", "node_id int32 0", "host string myhost", "port int32 34000", "[response_tagged_fields]"},
		},
		{name: "v3, response_tagged_fields", apiVersion: int16(3),
			hexInput: "000000000000054e4f4e45000000000a6c6f63616c686f73740000270f0100024711",
			expected: []string{"throttle_time_ms int32 0", "error_code int16 0", "error_message *string NONE", "coordinator struct", "node_id int32 0", "host string localhost", "port int32 9999", "[response_tagged_fields]", "response_tagged_fields tag 0 value 0x4711"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "error_code int16 0", "error_message *string NONE", "coordinator struct", "node_id int32 0", "host string myhost", "port int32 34000", "[response_tagged_fields]", "response_tagged_fields tag 0 value 0x4711"},
		},
		{name: "v3, response_tagged_fields with error", apiVersion: int16(3),
			hexInput: "0000000000081d5468652062726f6b6572206973206e6f7420617661696c61626c652e000000000a6c6f63616c686f73740000270f0100024711",
			expected: []string{"throttle_time_ms int32 0", "error_code int16 8", "error_message *string The broker is not available.", "coordinator struct", "node_id int32 0", "host string localhost", "port int32 9999", "[response_tagged_fields]", "response_tagged_fields tag 0 value 0x4711"},
			modifier: testResponseModifier2,
			modified: []string{"throttle_time_ms int32 0", "error_code int16 8", "error_message *string The broker is not available.", "coordinator struct", "node_id int32 0", "host string myhost", "port int32 34000", "[response_tagged_fields]", "response_tagged_fields tag 0 value 0x4711"},
		},
	}
	for _, tc := range tt {
		bytes, err := hex.DecodeString(tc.hexInput)
		if err != nil {
			t.Fatal(err)
		}
		schema := findCoordinatorResponseSchemaVersions[tc.apiVersion]

		a := assert.New(t)
		s, err := DecodeSchema(bytes, schema)
		if err != nil {
			t.Fatal(err)
		}
		dc := NewDecodeCheck()
		err = dc.Traverse(s)
		if err != nil {
			t.Fatal(err)
		}
		a.Equal(tc.expected, dc.AttrValues(), "decode:"+tc.name)

		// encode
		resp, err := EncodeSchema(s, schema)
		if err != nil {
			t.Fatal(err)
		}
		a.Equal(bytes, resp, "encode:"+tc.name)

		// modify
		modifier, err := GetResponseModifier(apiKeyFindCoordinator, tc.apiVersion, tc.modifier)
		if err != nil {
			t.Fatal(err)
		}
		resp, err = modifier.Apply(resp)
		if err != nil {
			t.Fatal(err)
		}
		s, err = DecodeSchema(resp, schema)
		if err != nil {
			t.Fatal(err)
		}
		dc = NewDecodeCheck()
		err = dc.Traverse(s)
		if err != nil {
			t.Fatal(err)
		}
		a.Equal(tc.modified, dc.AttrValues(), "modify:"+tc.name)
	}
}

type decodeCheck struct {
	attrValues []string
}

func NewDecodeCheck() *decodeCheck {
	return &decodeCheck{attrValues: make([]string, 0)}
}

func (t *decodeCheck) Traverse(s *Struct) error {
	for i, _ := range s.GetSchema().GetFields() {
		arg := s.Values[i]
		if err := t.value(s, arg, i); err != nil {
			return err
		}
	}
	return nil
}

func (t *decodeCheck) value(s *Struct, arg interface{}, sindex int) error {
	name := s.GetSchema().GetFields()[sindex].GetDef().GetName()

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
		if v != nil {
			t.append(name, "*string", *v)
		} else {
			t.append(name, "*string", nil)
		}
	case *Struct:
		t.append(name, "struct")
		err := t.Traverse(v)
		if err != nil {
			return err
		}
	case []interface{}:
		t.append(fmt.Sprintf("[%s]", name))
		for _, v := range v {
			if err := t.value(s, v, sindex); err != nil {
				return err
			}
		}
	case rawTaggedField:
		t.append(name, "tag", v.tag, "value", fmt.Sprintf("0x%s", hex.EncodeToString(v.data)))
	case []rawTaggedField:
		t.append(fmt.Sprintf("[%s]", name))
		for _, v := range v {
			if err := t.value(s, v, sindex); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unknow type for value %v: %v", arg, reflect.TypeOf(arg))
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
