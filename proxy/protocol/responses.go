package protocol

import (
	"errors"
	"fmt"

	"github.com/grepplabs/kafka-proxy/config"
)

const (
	apiKeyMetadata        = 3
	apiKeyFindCoordinator = 10
	apiKeyDescribeCluster = 60

	brokersKeyName = "brokers"
	brokerKeyName  = "broker_id"
	hostKeyName    = "host"
	portKeyName    = "port"
	nodeKeyName    = "node_id"

	coordinatorKeyName  = "coordinator"
	coordinatorsKeyName = "coordinators"
)

var (
	metadataResponseSchemaVersions        = createMetadataResponseSchemaVersions()
	findCoordinatorResponseSchemaVersions = createFindCoordinatorResponseSchemaVersions()
	describeClusterResponseSchemaVersions = createDescribeClusterResponseSchemaVersions()
)

func createMetadataResponseSchemaVersions() []Schema {
	metadataBrokerV0 := NewSchema("metadata_broker_v0",
		&Mfield{Name: nodeKeyName, Ty: TypeInt32},
		&Mfield{Name: hostKeyName, Ty: TypeStr},
		&Mfield{Name: portKeyName, Ty: TypeInt32},
	)

	partitionMetadataV0 := NewSchema("partition_metadata_v0",
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "partition", Ty: TypeInt32},
		&Mfield{Name: "leader", Ty: TypeInt32},
		&Array{Name: "replicas", Ty: TypeInt32},
		&Array{Name: "isr", Ty: TypeInt32},
	)

	topicMetadataV0 := NewSchema("topic_metadata_v0",
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "topic", Ty: TypeStr},
		&Array{Name: "partition_metadata", Ty: partitionMetadataV0},
	)

	metadataResponseV0 := NewSchema("metadata_response_v0",
		&Array{Name: brokersKeyName, Ty: metadataBrokerV0},
		&Array{Name: "topic_metadata", Ty: topicMetadataV0},
	)

	metadataBrokerV1 := NewSchema("metadata_broker_v1",
		&Mfield{Name: nodeKeyName, Ty: TypeInt32},
		&Mfield{Name: hostKeyName, Ty: TypeStr},
		&Mfield{Name: portKeyName, Ty: TypeInt32},
		&Mfield{Name: "rack", Ty: TypeNullableStr},
	)

	metadataBrokerSchema9 := NewSchema("metadata_broker_schema9",
		&Mfield{Name: nodeKeyName, Ty: TypeInt32},
		&Mfield{Name: hostKeyName, Ty: TypeCompactStr},
		&Mfield{Name: portKeyName, Ty: TypeInt32},
		&Mfield{Name: "rack", Ty: TypeCompactNullableStr},
		&SchemaTaggedFields{"broker_tagged_fields"},
	)

	partitionMetadataV1 := partitionMetadataV0

	partitionMetadataV2 := NewSchema("partition_metadata_v2",
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "partition", Ty: TypeInt32},
		&Mfield{Name: "leader", Ty: TypeInt32},
		&Array{Name: "replicas", Ty: TypeInt32},
		&Array{Name: "isr", Ty: TypeInt32},
		&Array{Name: "offline_replicas", Ty: TypeInt32},
	)

	partitionMetadataV7 := NewSchema("partition_metadata_v7",
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "partition", Ty: TypeInt32},
		&Mfield{Name: "leader", Ty: TypeInt32},
		&Mfield{Name: "leader_epoch", Ty: TypeInt32},
		&Array{Name: "replicas", Ty: TypeInt32},
		&Array{Name: "isr", Ty: TypeInt32},
		&Array{Name: "offline_replicas", Ty: TypeInt32},
	)

	partitionMetadataSchema9 := NewSchema("partition_metadata_schema9",
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "partition", Ty: TypeInt32},
		&Mfield{Name: "leader", Ty: TypeInt32},
		&Mfield{Name: "leader_epoch", Ty: TypeInt32},
		&CompactArray{Name: "replicas", Ty: TypeInt32},
		&CompactArray{Name: "isr", Ty: TypeInt32},
		&CompactArray{Name: "offline_replicas", Ty: TypeInt32},
		&SchemaTaggedFields{Name: "partition_metadata_tagged_fields"},
	)

	topicMetadataV1 := NewSchema("topic_metadata_v1",
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "topic", Ty: TypeStr},
		&Mfield{Name: "is_internal", Ty: TypeBool},
		&Array{Name: "partition_metadata", Ty: partitionMetadataV1},
	)

	topicMetadataV2 := NewSchema("topic_metadata_v2",
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "topic", Ty: TypeStr},
		&Mfield{Name: "is_internal", Ty: TypeBool},
		&Array{Name: "partition_metadata", Ty: partitionMetadataV2},
	)

	topicMetadataV7 := NewSchema("topic_metadata_v7",
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "topic", Ty: TypeStr},
		&Mfield{Name: "is_internal", Ty: TypeBool},
		&Array{Name: "partition_metadata", Ty: partitionMetadataV7},
	)

	topicMetadataV8 := NewSchema("topic_metadata_v8",
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "name", Ty: TypeStr},
		&Mfield{Name: "is_internal", Ty: TypeBool},
		&Array{Name: "partition_metadata", Ty: partitionMetadataV7},
		&Mfield{Name: "topic_authorized_operations", Ty: TypeInt32},
	)

	topicMetadataSchema9 := NewSchema("topic_metadata_schema9",
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "name", Ty: TypeCompactStr},
		&Mfield{Name: "is_internal", Ty: TypeBool},
		&CompactArray{Name: "partition_metadata", Ty: partitionMetadataSchema9},
		&Mfield{Name: "topic_authorized_operations", Ty: TypeInt32},
		&SchemaTaggedFields{Name: "topic_metadata_tagged_fields"},
	)

	topicMetadataSchema10 := NewSchema("topic_metadata_schema10",
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "name", Ty: TypeCompactStr},
		&Mfield{Name: "topic_id", Ty: TypeUuid},
		&Mfield{Name: "is_internal", Ty: TypeBool},
		&CompactArray{Name: "partition_metadata", Ty: partitionMetadataSchema9},
		&Mfield{Name: "topic_authorized_operations", Ty: TypeInt32},
		&SchemaTaggedFields{Name: "topic_metadata_tagged_fields"},
	)

	topicMetadataSchema12 := NewSchema("topic_metadata_schema12",
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "name", Ty: TypeCompactNullableStr},
		&Mfield{Name: "topic_id", Ty: TypeUuid},
		&Mfield{Name: "is_internal", Ty: TypeBool},
		&CompactArray{Name: "partition_metadata", Ty: partitionMetadataSchema9},
		&Mfield{Name: "topic_authorized_operations", Ty: TypeInt32},
		&SchemaTaggedFields{Name: "topic_metadata_tagged_fields"},
	)

	metadataResponseV1 := NewSchema("metadata_response_v1",
		&Array{Name: brokersKeyName, Ty: metadataBrokerV1},
		&Mfield{Name: "controller_id", Ty: TypeInt32},
		&Array{Name: "topic_metadata", Ty: topicMetadataV1},
	)

	metadataResponseV2 := NewSchema("metadata_response_v2",
		&Array{Name: brokersKeyName, Ty: metadataBrokerV1},
		&Mfield{Name: "cluster_id", Ty: TypeNullableStr},
		&Mfield{Name: "controller_id", Ty: TypeInt32},
		&Array{Name: "topic_metadata", Ty: topicMetadataV1},
	)

	metadataResponseV3 := NewSchema("metadata_response_v3",
		&Mfield{Name: "throttle_time_ms", Ty: TypeInt32},
		&Array{Name: brokersKeyName, Ty: metadataBrokerV1},
		&Mfield{Name: "cluster_id", Ty: TypeNullableStr},
		&Mfield{Name: "controller_id", Ty: TypeInt32},
		&Array{Name: "topic_metadata", Ty: topicMetadataV1},
	)

	metadataResponseV4 := metadataResponseV3

	metadataResponseV5 := NewSchema("metadata_response_v5",
		&Mfield{Name: "throttle_time_ms", Ty: TypeInt32},
		&Array{Name: brokersKeyName, Ty: metadataBrokerV1},
		&Mfield{Name: "cluster_id", Ty: TypeNullableStr},
		&Mfield{Name: "controller_id", Ty: TypeInt32},
		&Array{Name: "topic_metadata", Ty: topicMetadataV2},
	)

	metadataResponseV6 := metadataResponseV5

	metadataResponseV7 := NewSchema("metadata_response_v7",
		&Mfield{Name: "throttle_time_ms", Ty: TypeInt32},
		&Array{Name: brokersKeyName, Ty: metadataBrokerV1},
		&Mfield{Name: "cluster_id", Ty: TypeNullableStr},
		&Mfield{Name: "controller_id", Ty: TypeInt32},
		&Array{Name: "topic_metadata", Ty: topicMetadataV7},
	)

	metadataResponseV8 := NewSchema("metadata_response_v8",
		&Mfield{Name: "throttle_time_ms", Ty: TypeInt32},
		&Array{Name: brokersKeyName, Ty: metadataBrokerV1},
		&Mfield{Name: "cluster_id", Ty: TypeNullableStr},
		&Mfield{Name: "controller_id", Ty: TypeInt32},
		&Array{Name: "topic_metadata", Ty: topicMetadataV8},
		&Mfield{Name: "cluster_authorized_operations", Ty: TypeInt32},
	)

	metadataResponseV9 := NewSchema("metadata_response_v9",
		&Mfield{Name: "throttle_time_ms", Ty: TypeInt32},
		&CompactArray{Name: brokersKeyName, Ty: metadataBrokerSchema9},
		&Mfield{Name: "cluster_id", Ty: TypeCompactNullableStr},
		&Mfield{Name: "controller_id", Ty: TypeInt32},
		&CompactArray{Name: "topic_metadata", Ty: topicMetadataSchema9},
		&Mfield{Name: "cluster_authorized_operations", Ty: TypeInt32},
		&SchemaTaggedFields{Name: "response_tagged_fields"},
	)

	metadataResponseV10 := NewSchema("metadata_response_v10",
		&Mfield{Name: "throttle_time_ms", Ty: TypeInt32},
		&CompactArray{Name: brokersKeyName, Ty: metadataBrokerSchema9},
		&Mfield{Name: "cluster_id", Ty: TypeCompactNullableStr},
		&Mfield{Name: "controller_id", Ty: TypeInt32},
		&CompactArray{Name: "topic_metadata", Ty: topicMetadataSchema10},
		&Mfield{Name: "cluster_authorized_operations", Ty: TypeInt32},
		&SchemaTaggedFields{Name: "response_tagged_fields"},
	)

	metadataResponseV11 := NewSchema("metadata_response_v11",
		&Mfield{Name: "throttle_time_ms", Ty: TypeInt32},
		&CompactArray{Name: brokersKeyName, Ty: metadataBrokerSchema9},
		&Mfield{Name: "cluster_id", Ty: TypeCompactNullableStr},
		&Mfield{Name: "controller_id", Ty: TypeInt32},
		&CompactArray{Name: "topic_metadata", Ty: topicMetadataSchema10},
		&SchemaTaggedFields{Name: "response_tagged_fields"},
	)

	metadataResponseV12 := NewSchema("metadata_response_v12",
		&Mfield{Name: "throttle_time_ms", Ty: TypeInt32},
		&CompactArray{Name: brokersKeyName, Ty: metadataBrokerSchema9},
		&Mfield{Name: "cluster_id", Ty: TypeCompactNullableStr},
		&Mfield{Name: "controller_id", Ty: TypeInt32},
		&CompactArray{Name: "topic_metadata", Ty: topicMetadataSchema12},
		&SchemaTaggedFields{Name: "response_tagged_fields"},
	)

	metadataResponseV13 := NewSchema("metadata_response_v13",
		&Mfield{Name: "throttle_time_ms", Ty: TypeInt32},
		&CompactArray{Name: brokersKeyName, Ty: metadataBrokerSchema9},
		&Mfield{Name: "cluster_id", Ty: TypeCompactNullableStr},
		&Mfield{Name: "controller_id", Ty: TypeInt32},
		&CompactArray{Name: "topic_metadata", Ty: topicMetadataSchema12},
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&SchemaTaggedFields{Name: "response_tagged_fields"},
	)

	return []Schema{
		metadataResponseV0,
		metadataResponseV1,
		metadataResponseV2,
		metadataResponseV3,
		metadataResponseV4,
		metadataResponseV5,
		metadataResponseV6,
		metadataResponseV7,
		metadataResponseV8,
		metadataResponseV9,
		metadataResponseV10,
		metadataResponseV11,
		metadataResponseV12,
		metadataResponseV13,
	}
}

func createFindCoordinatorResponseSchemaVersions() []Schema {
	findCoordinatorBrokerV0 := NewSchema("find_coordinator_broker_v0",
		&Mfield{Name: nodeKeyName, Ty: TypeInt32},
		&Mfield{Name: hostKeyName, Ty: TypeStr},
		&Mfield{Name: portKeyName, Ty: TypeInt32},
	)

	findCoordinatorBrokerSchema9 := NewSchema("find_coordinator_broker_schema9",
		&Mfield{Name: nodeKeyName, Ty: TypeInt32},
		&Mfield{Name: hostKeyName, Ty: TypeCompactStr},
		&Mfield{Name: portKeyName, Ty: TypeInt32},
	)

	findCoordinatorResponseV0 := NewSchema("find_coordinator_response_v0",
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: coordinatorKeyName, Ty: findCoordinatorBrokerV0},
	)

	findCoordinatorResponseV1 := NewSchema("find_coordinator_response_v1",
		&Mfield{Name: "throttle_time_ms", Ty: TypeInt32},
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "error_message", Ty: TypeNullableStr},
		&Mfield{Name: coordinatorKeyName, Ty: findCoordinatorBrokerV0},
	)

	findCoordinatorResponseV2 := findCoordinatorResponseV1

	findCoordinatorResponseV3 := NewSchema("find_coordinator_response_v3",
		&Mfield{Name: "throttle_time_ms", Ty: TypeInt32},
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "error_message", Ty: TypeCompactNullableStr},
		&Mfield{Name: coordinatorKeyName, Ty: findCoordinatorBrokerSchema9},
		&SchemaTaggedFields{Name: "response_tagged_fields"},
	)

	findCoordinatorCoordinatorsSchema4 := NewSchema("find_coordinator_coordinators_schema4",
		&Mfield{Name: "key", Ty: TypeCompactStr},
		&Mfield{Name: coordinatorKeyName, Ty: findCoordinatorBrokerSchema9},
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "error_message", Ty: TypeCompactNullableStr},
		&SchemaTaggedFields{"coordinators_tagged_fields"},
	)
	findCoordinatorResponseV4 := NewSchema("find_coordinator_response_v4",
		&Mfield{Name: "throttle_time_ms", Ty: TypeInt32},
		&CompactArray{Name: coordinatorsKeyName, Ty: findCoordinatorCoordinatorsSchema4},
		&SchemaTaggedFields{Name: "response_tagged_fields"},
	)
	findCoordinatorResponseV5 := findCoordinatorResponseV4
	findCoordinatorResponseV6 := findCoordinatorResponseV5

	return []Schema{findCoordinatorResponseV0, findCoordinatorResponseV1, findCoordinatorResponseV2, findCoordinatorResponseV3, findCoordinatorResponseV4, findCoordinatorResponseV5, findCoordinatorResponseV6}
}

func createDescribeClusterResponseSchemaVersions() []Schema {
	describeClusterBrokerV0 := NewSchema("describe_cluster_broker_v0",
		&Mfield{Name: brokerKeyName, Ty: TypeInt32},
		&Mfield{Name: hostKeyName, Ty: TypeStr},
		&Mfield{Name: portKeyName, Ty: TypeInt32},
		&Mfield{Name: "rack", Ty: TypeNullableStr},
	)

	describeClusterBrokerV2 := NewSchema("describe_cluster_broker_v2",
		&Mfield{Name: brokerKeyName, Ty: TypeInt32},
		&Mfield{Name: hostKeyName, Ty: TypeStr},
		&Mfield{Name: portKeyName, Ty: TypeInt32},
		&Mfield{Name: "rack", Ty: TypeNullableStr},
		&Mfield{Name: "is_fenced", Ty: TypeBool},
	)

	describeClusterV0 := NewSchema("describe_cluster_response_v0",
		&Mfield{Name: "throttle_time_ms", Ty: TypeInt32},
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "error_message", Ty: TypeNullableStr},
		&Mfield{Name: "cluster_id", Ty: TypeStr},
		&Mfield{Name: "controller_id", Ty: TypeInt32},
		&Array{Name: brokersKeyName, Ty: describeClusterBrokerV0},
		&Mfield{Name: "cluster_authorized_operations", Ty: TypeInt32},
		&SchemaTaggedFields{Name: "response_tagged_fields"},
	)

	describeClusterV1 := NewSchema("describe_cluster_response_v1",
		&Mfield{Name: "throttle_time_ms", Ty: TypeInt32},
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "error_message", Ty: TypeNullableStr},
		&Mfield{Name: "endpoint_type", Ty: TypeInt8},
		&Mfield{Name: "cluster_id", Ty: TypeStr},
		&Mfield{Name: "controller_id", Ty: TypeInt32},
		&Array{Name: brokersKeyName, Ty: describeClusterBrokerV0},
		&Mfield{Name: "cluster_authorized_operations", Ty: TypeInt32},
		&SchemaTaggedFields{Name: "response_tagged_fields"},
	)

	describeClusterV2 := NewSchema("describe_cluster_response_v2",
		&Mfield{Name: "throttle_time_ms", Ty: TypeInt32},
		&Mfield{Name: "error_code", Ty: TypeInt16},
		&Mfield{Name: "error_message", Ty: TypeNullableStr},
		&Mfield{Name: "endpoint_type", Ty: TypeInt8},
		&Mfield{Name: "cluster_id", Ty: TypeStr},
		&Mfield{Name: "controller_id", Ty: TypeInt32},
		&Array{Name: brokersKeyName, Ty: describeClusterBrokerV2},
		&Mfield{Name: "cluster_authorized_operations", Ty: TypeInt32},
		&SchemaTaggedFields{Name: "response_tagged_fields"},
	)
	return []Schema{describeClusterV0, describeClusterV1, describeClusterV2}
}

func modifyMetadataResponse(decodedStruct *Struct, fn config.NetAddressMappingFunc) error {
	if decodedStruct == nil {
		return errors.New("decoded struct must not be nil")
	}
	if fn == nil {
		return errors.New("net address mapper must not be nil")
	}
	brokersArray, ok := decodedStruct.Get(brokersKeyName).([]interface{})
	if !ok {
		return errors.New("brokers list not found")
	}
	for _, brokerElement := range brokersArray {
		broker := brokerElement.(*Struct)
		host, ok := broker.Get(hostKeyName).(string)
		if !ok {
			return errors.New("broker.host not found")
		}
		port, ok := broker.Get(portKeyName).(int32)
		if !ok {
			return errors.New("broker.port not found")
		}
		nodeId, ok := broker.Get(nodeKeyName).(int32)
		if !ok {
			return errors.New("broker.node_id not found")
		}

		if host == "" && port <= 0 {
			continue
		}

		newHost, newPort, err := fn(host, port, nodeId)
		if err != nil {
			return err
		}
		if host != newHost {
			err := broker.Replace(hostKeyName, newHost)
			if err != nil {
				return err
			}
		}
		if port != newPort {
			err = broker.Replace(portKeyName, newPort)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func modifyFindCoordinatorResponse(decodedStruct *Struct, fn config.NetAddressMappingFunc) error {
	if decodedStruct == nil {
		return errors.New("decoded struct must not be nil")
	}
	if fn == nil {
		return errors.New("net address mapper must not be nil")
	}
	coordinators := decodedStruct.Get(coordinatorsKeyName)
	if coordinators != nil {
		coordinatorsArray, ok := coordinators.([]interface{})
		if !ok {
			return errors.New("coordinators list not found")
		}
		for _, coordinatorElement := range coordinatorsArray {
			coordinatorStruct := coordinatorElement.(*Struct)
			if err := modifyCoordinator(coordinatorStruct, fn); err != nil {
				return err
			}
		}
		return nil
	} else {
		return modifyCoordinator(decodedStruct, fn)
	}
}

func modifyCoordinator(decodedStruct *Struct, fn config.NetAddressMappingFunc) error {
	coordinator, ok := decodedStruct.Get(coordinatorKeyName).(*Struct)
	if !ok {
		return errors.New("coordinator not found")
	}
	host, ok := coordinator.Get(hostKeyName).(string)
	if !ok {
		return errors.New("coordinator.host not found")
	}
	port, ok := coordinator.Get(portKeyName).(int32)
	if !ok {
		return errors.New("coordinator.port not found")
	}
	nodeId, ok := coordinator.Get(nodeKeyName).(int32)
	if !ok {
		return errors.New("coordinator.node_id not found")
	}

	if host == "" && port <= 0 {
		return nil
	}

	newHost, newPort, err := fn(host, port, nodeId)
	if err != nil {
		return err
	}
	if host != newHost {
		err := coordinator.Replace(hostKeyName, newHost)
		if err != nil {
			return err
		}
	}
	if port != newPort {
		err = coordinator.Replace(portKeyName, int32(newPort))
		if err != nil {
			return err
		}
	}
	return nil
}

func modifyDescribeClusterResponse(decodedStruct *Struct, fn config.NetAddressMappingFunc) error {
	if decodedStruct == nil {
		return errors.New("decoded struct must not be nil")
	}
	if fn == nil {
		return errors.New("net address mapper must not be nil")
	}
	brokersArray, ok := decodedStruct.Get(brokersKeyName).([]interface{})
	if !ok {
		return errors.New("brokers list not found")
	}
	for _, brokerElement := range brokersArray {
		broker := brokerElement.(*Struct)
		host, ok := broker.Get(hostKeyName).(string)
		if !ok {
			return errors.New("broker.host not found")
		}
		port, ok := broker.Get(portKeyName).(int32)
		if !ok {
			return errors.New("broker.port not found")
		}
		brokerId, ok := broker.Get(brokerKeyName).(int32)
		if !ok {
			return errors.New("broker.broker_id not found")
		}

		if host == "" && port <= 0 {
			continue
		}

		newHost, newPort, err := fn(host, port, brokerId)
		if err != nil {
			return err
		}
		if host != newHost {
			err := broker.Replace(hostKeyName, newHost)
			if err != nil {
				return err
			}
		}
		if port != newPort {
			err = broker.Replace(portKeyName, newPort)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type ResponseModifier interface {
	Apply(resp []byte) ([]byte, error)
}

type modifyResponseFunc func(decodedStruct *Struct, fn config.NetAddressMappingFunc) error

type responseModifier struct {
	schema                Schema
	modifyResponseFunc    modifyResponseFunc
	netAddressMappingFunc config.NetAddressMappingFunc
}

func (f *responseModifier) Apply(resp []byte) ([]byte, error) {
	decodedStruct, err := DecodeSchema(resp, f.schema)
	if err != nil {
		return nil, err
	}
	err = f.modifyResponseFunc(decodedStruct, f.netAddressMappingFunc)
	if err != nil {
		return nil, err
	}
	return EncodeSchema(decodedStruct, f.schema)
}

func GetResponseModifier(apiKey int16, apiVersion int16, addressMappingFunc config.NetAddressMappingFunc) (ResponseModifier, error) {
	switch apiKey {
	case apiKeyMetadata:
		return newResponseModifier(apiKey, apiVersion, addressMappingFunc, metadataResponseSchemaVersions, modifyMetadataResponse)
	case apiKeyFindCoordinator:
		return newResponseModifier(apiKey, apiVersion, addressMappingFunc, findCoordinatorResponseSchemaVersions, modifyFindCoordinatorResponse)
	case apiKeyDescribeCluster:
		return newResponseModifier(apiKey, apiVersion, addressMappingFunc, describeClusterResponseSchemaVersions, modifyDescribeClusterResponse)
	default:
		return nil, nil
	}
}

func newResponseModifier(apiKey int16, apiVersion int16, netAddressMappingFunc config.NetAddressMappingFunc, schemas []Schema, modifyResponseFunc modifyResponseFunc) (ResponseModifier, error) {
	schema, err := getResponseSchema(apiKey, apiVersion, schemas)
	if err != nil {
		return nil, err
	}
	return &responseModifier{
		schema:                schema,
		modifyResponseFunc:    modifyResponseFunc,
		netAddressMappingFunc: netAddressMappingFunc,
	}, nil
}

func getResponseSchema(apiKey, apiVersion int16, schemas []Schema) (Schema, error) {
	if apiVersion < 0 || int(apiVersion) >= len(schemas) {
		return nil, fmt.Errorf("Unsupported response schema version %d for key %d ", apiVersion, apiKey)
	}
	return schemas[apiVersion], nil
}
