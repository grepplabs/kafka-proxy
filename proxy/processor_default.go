package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
	"github.com/sirupsen/logrus"
)

// Map of API keys to their operation names
var apiKeyNames = map[int16]string{
	0:  "Produce",
	1:  "Fetch",
	2:  "ListOffsets",
	3:  "Metadata",
	4:  "LeaderAndIsr",
	5:  "StopReplica",
	6:  "UpdateMetadata",
	7:  "ControlledShutdown",
	8:  "OffsetCommit",
	9:  "OffsetFetch",
	10: "FindCoordinator",
	11: "JoinGroup",
	12: "Heartbeat",
	13: "LeaveGroup",
	14: "SyncGroup",
	15: "DescribeGroups",
	16: "ListGroups",
	17: "SaslHandshake",
	18: "ApiVersions",
	19: "CreateTopics",
	20: "DeleteTopics",
	21: "DeleteRecords",
	22: "InitProducerId",
	23: "OffsetForLeaderEpoch",
	24: "AddPartitionsToTxn",
	25: "AddOffsetsToTxn",
	26: "EndTxn",
	27: "WriteTxnMarkers",
	28: "TxnOffsetCommit",
	29: "DescribeAcls",
	30: "CreateAcls",
	31: "DeleteAcls",
	32: "DescribeConfigs",
	33: "AlterConfigs",
	34: "AlterReplicaLogDirs",
	35: "DescribeLogDirs",
	36: "SaslAuthenticate",
	37: "CreatePartitions",
	38: "CreateDelegationToken",
	39: "RenewDelegationToken",
	40: "ExpireDelegationToken",
	41: "DescribeDelegationToken",
	42: "DeleteGroups",
	43: "ElectLeaders",
	44: "IncrementalAlterConfigs",
	45: "AlterPartitionReassignments",
	46: "ListPartitionReassignments",
	47: "OffsetDelete",
	48: "DescribeClientQuotas",
	49: "AlterClientQuotas",
	50: "DescribeUserScramCredentials",
	51: "AlterUserScramCredentials",
	52: "Vote",
	53: "BeginQuorumEpoch",
	54: "EndQuorumEpoch",
	55: "DescribeQuorum",
	56: "AlterIsr",
	57: "UpdateFeatures",
	58: "Envelope",
	59: "FetchSnapshot",
	60: "DescribeCluster",
	61: "DescribeProducers",
	62: "BrokerRegistration",
	63: "BrokerHeartbeat",
}

// Map API keys to their resource types and operations
// Based on https://docs.confluent.io/platform/current/security/authorization/acls/overview.html
var apiKeyResources = map[int16]struct {
	ResourceType string
	Operation    string
}{
	// Cluster operations
	3:  {"Cluster", "Create"},          // Metadata
	4:  {"Cluster", "ClusterAction"},   // LeaderAndIsr
	5:  {"Cluster", "ClusterAction"},   // StopReplica
	6:  {"Cluster", "ClusterAction"},   // UpdateMetadata
	7:  {"Cluster", "ClusterAction"},   // ControlledShutdown
	19: {"Cluster", "Create"},          // CreateTopics
	23: {"Cluster", "ClusterAction"},   // OffsetForLeaderEpoch
	27: {"Cluster", "ClusterAction"},   // WriteTxnMarkers
	29: {"Cluster", "Describe"},        // DescribeAcls
	30: {"Cluster", "Alter"},           // CreateAcls
	31: {"Cluster", "Alter"},           // DeleteAcls
	32: {"Cluster", "DescribeConfigs"}, // DescribeConfigs
	33: {"Cluster", "AlterConfigs"},    // AlterConfigs
	34: {"Cluster", "Alter"},           // AlterReplicaLogDirs
	35: {"Cluster", "Describe"},        // DescribeLogDirs
	16: {"Cluster", "Describe"},        // ListGroups

	// Topic operations
	0:  {"Topic", "Write"},    // Produce
	1:  {"Topic", "Read"},     // Fetch
	2:  {"Topic", "Describe"}, // ListOffsets
	20: {"Topic", "Delete"},   // DeleteTopics
	21: {"Topic", "Delete"},   // DeleteRecords
	24: {"Topic", "Write"},    // AddPartitionsToTxn
	37: {"Topic", "Alter"},    // CreatePartitions
	9:  {"Topic", "Describe"}, // OffsetFetch
	8:  {"Topic", "Read"},     // OffsetCommit
	28: {"Topic", "Read"},     // TxnOffsetCommit

	// Group operations
	10: {"Group", "Describe"}, // FindCoordinator
	11: {"Group", "Read"},     // JoinGroup
	12: {"Group", "Read"},     // Heartbeat
	13: {"Group", "Read"},     // LeaveGroup
	14: {"Group", "Read"},     // SyncGroup
	15: {"Group", "Describe"}, // DescribeGroups
	25: {"Group", "Read"},     // AddOffsetsToTxn
	42: {"Group", "Delete"},   // DeleteGroups

	// TransactionalId operations
	26: {"TransactionalId", "Write"}, // EndTxn
	22: {"TransactionalId", "Write"}, // InitProducerId

	// DelegationToken operations
	41: {"DelegationToken", "Describe"}, // DescribeTokens
}

func getOperationName(apiKey int16) string {
	if name, ok := apiKeyNames[apiKey]; ok {
		return name
	}
	return "Unknown"
}

func getResourceOperation(apiKey int16) string {
	if resource, ok := apiKeyResources[apiKey]; ok {
		return fmt.Sprintf("%s/%s", resource.ResourceType, resource.Operation)
	}
	return "Unknown/Unknown"
}

type DefaultRequestHandler struct {
}

type DefaultResponseHandler struct {
}

func (handler *DefaultRequestHandler) handleRequest(dst DeadlineWriter, src DeadlineReaderWriter, ctx *RequestsLoopContext) (readErr bool, err error) {
	// logrus.Println("Await Kafka request")

	// waiting for first bytes or EOF - reset deadlines
	if err = src.SetReadDeadline(time.Time{}); err != nil {
		return true, err
	}
	if err = dst.SetWriteDeadline(time.Time{}); err != nil {
		return true, err
	}

	keyVersionBuf := make([]byte, 8) // Size => int32 + ApiKey => int16 + ApiVersion => int16

	if _, err = io.ReadFull(src, keyVersionBuf); err != nil {
		return true, err
	}

	requestKeyVersion := &protocol.RequestKeyVersion{}
	if err = protocol.Decode(keyVersionBuf, requestKeyVersion); err != nil {
		return true, err
	}
	logrus.Debugf("Kafka request operation: %s (key: %v, version: %v, length: %v), resource operation: %s",
		getOperationName(requestKeyVersion.ApiKey),
		requestKeyVersion.ApiKey,
		requestKeyVersion.ApiVersion,
		requestKeyVersion.Length,
		getResourceOperation(requestKeyVersion.ApiKey))

	if requestKeyVersion.ApiKey < minRequestApiKey || requestKeyVersion.ApiKey > maxRequestApiKey {
		return true, fmt.Errorf("api key %d is invalid, possible cause: using plain connection instead of TLS", requestKeyVersion.ApiKey)
	}

	proxyRequestsTotal.WithLabelValues(ctx.brokerAddress, strconv.Itoa(int(requestKeyVersion.ApiKey)), strconv.Itoa(int(requestKeyVersion.ApiVersion))).Inc()
	proxyRequestsBytes.WithLabelValues(ctx.brokerAddress).Add(float64(requestKeyVersion.Length + 4))

	if _, ok := ctx.forbiddenApiKeys[requestKeyVersion.ApiKey]; ok {
		return true, fmt.Errorf("api key %d (%s) is forbidden", requestKeyVersion.ApiKey, getOperationName(requestKeyVersion.ApiKey))
	}

	if ctx.localSasl.enabled {
		if ctx.localSaslDone {
			if requestKeyVersion.ApiKey == apiKeySaslHandshake {
				return false, errors.New("SASL Auth was already done")
			}
		} else {
			switch requestKeyVersion.ApiKey {
			case apiKeySaslHandshake:
				switch requestKeyVersion.ApiVersion {
				case 0:
					if err = ctx.localSasl.receiveAndSendSASLAuthV0(src, keyVersionBuf); err != nil {
						return true, err
					}
				case 1:
					if err = ctx.localSasl.receiveAndSendSASLAuthV1(src, keyVersionBuf); err != nil {
						return true, err
					}
				default:
					return true, fmt.Errorf("only saslHandshake version 0 and 1 are supported, got version %d", requestKeyVersion.ApiVersion)
				}
				ctx.localSaslDone = true
				if err = src.SetDeadline(time.Time{}); err != nil {
					return false, err
				}
				// defaultRequestHandler was consumed but due to local handling enqueued defaultResponseHandler will not be.
				return false, ctx.putNextRequestHandler(defaultRequestHandler)
			case apiKeyApiApiVersions:
				// continue processing
			default:
				return false, errors.New("SASL Auth is required. Only SaslHandshake or ApiVersions requests are allowed")
			}
		}
	}

	mustReply, readBytes, topicNames, err := handler.mustReply(requestKeyVersion, src, ctx)
	if err != nil {
		return true, err
	}

	ctx.acl = &apis.ACLCollection{}

	for _, topicName := range topicNames {
		if topicName != "" {
			logrus.Debugf("Topic name: %s", topicName)
		}
	}

	if ctx.aclChecker != nil { // Check if ACLChecker is configured
		var allowed bool
		allowed, _, err = ctx.aclChecker.CheckACL(context.Background(), 0, topicNames) // Pass both requestKeyVersion and topic
		if err != nil {
			return true, err
		}
		if !allowed {
			return true, fmt.Errorf("access denied for operation %s (api key %d) on topic %q",
				getOperationName(requestKeyVersion.ApiKey),
				requestKeyVersion.ApiKey,
				topicNames)
		}
	}

	// send inFlightRequest to channel before myCopyN to prevent race condition in proxyResponses
	if mustReply {
		if err = sendRequestKeyVersion(ctx.openRequestsChannel, openRequestSendTimeout, requestKeyVersion); err != nil {
			return true, err
		}
	}

	requestDeadline := time.Now().Add(ctx.timeout)
	err = dst.SetWriteDeadline(requestDeadline)
	if err != nil {
		return false, err
	}
	err = src.SetReadDeadline(requestDeadline)
	if err != nil {
		return true, err
	}

	// write - send to broker
	if _, err = dst.Write(keyVersionBuf); err != nil {
		return false, err
	}
	// write - send to broker
	if len(readBytes) > 0 {
		if _, err = dst.Write(readBytes); err != nil {
			return false, err
		}
	}

	// 4 bytes were written as keyVersionBuf (ApiKey, ApiVersion)
	if readErr, err = myCopyN(dst, src, int64(requestKeyVersion.Length-int32(4+len(readBytes))), ctx.buf); err != nil {
		return readErr, err
	}
	if requestKeyVersion.ApiKey == apiKeySaslHandshake {
		if requestKeyVersion.ApiVersion == 0 {
			return false, ctx.putNextHandlers(saslAuthV0RequestHandler, saslAuthV0ResponseHandler)
		}
	}
	if mustReply {
		return false, ctx.putNextHandlers(defaultRequestHandler, defaultResponseHandler)
	} else {
		return false, ctx.putNextRequestHandler(defaultRequestHandler)
	}
}

func (handler *DefaultRequestHandler) mustReply(
	requestKeyVersion *protocol.RequestKeyVersion,
	src io.Reader,
	ctx *RequestsLoopContext,
) (bool, []byte, []string, error) {
	var (
		needReply  bool = true
		bufferRead bytes.Buffer
		topicNames []string
		err        error
	)

	reader := io.TeeReader(src, &bufferRead)

	logrus.Debugf("ResponseHeaderVersion: %v", requestKeyVersion.ResponseHeaderVersion())

	// Only parse headers for supported ApiKeys that need topic information
	switch requestKeyVersion.ApiKey {
	case apiKeyProduce, apiKeyFetch, apiKeyListOffsets, apiKeyCreateTopics, apiKeyDeleteTopics:
		// Read CorrelationID (INT32)
		if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
			return false, nil, nil, err
		}

		// Read ClientID (NULLABLE_STRING)
		if _, err = readNullableString(reader); err != nil {
			return false, nil, nil, err
		}

		if requestKeyVersion.ResponseHeaderVersion() == 1 {
			if err = readTaggedFields(reader); err != nil {
				return false, nil, nil, err
			}
		}
	default:
		return true, nil, nil, nil
	}

	switch requestKeyVersion.ApiKey {
	case apiKeyProduce:
		needReply, topicNames, err = handler.handleProduce(reader, requestKeyVersion, ctx)
	case apiKeyFetch:
		needReply, topicNames, err = handler.handleFetch(reader, requestKeyVersion)
	case apiKeyListOffsets:
		needReply, topicNames, err = handler.handleListOffsets(reader, requestKeyVersion)
	case apiKeyCreateTopics:
		needReply, topicNames, err = handler.handleCreateTopics(reader, requestKeyVersion)
	case apiKeyDeleteTopics:
		needReply, topicNames, err = handler.handleDeleteTopics(reader, requestKeyVersion)
	case apiKeyDeleteRecords:
		needReply, topicNames, err = handler.handleDeleteRecords(reader, requestKeyVersion)
	case apiKeyCreatePartitions:
		needReply, topicNames, err = handler.handleCreatePartitions(reader, requestKeyVersion)
	default:
		return true, nil, nil, nil
	}

	if err != nil {
		logrus.Errorf("Error processing request: %v", err)
		return false, nil, nil, err
	}

	return needReply, bufferRead.Bytes(), topicNames, nil
}

func (handler *DefaultRequestHandler) handleProduce(
	reader io.Reader,
	requestKeyVersion *protocol.RequestKeyVersion,
	ctx *RequestsLoopContext,
) (bool, []string, error) {
	var (
		acks       int16
		topicNames []string
		err        error
	)

	// Read transactional_id
	if requestKeyVersion.ApiVersion >= 3 {
		if requestKeyVersion.ApiVersion >= 9 {
			if _, err = readCompactNullableString(reader); err != nil {
				return false, nil, err
			}
		} else {
			if _, err = readNullableString(reader); err != nil {
				return false, nil, err
			}
		}
	}

	// Read acks and timeout_ms
	if err = binary.Read(reader, binary.BigEndian, &acks); err != nil {
		return false, nil, err
	}
	if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
		return false, nil, err
	}

	// Read topics array
	var topicsCount int32
	if requestKeyVersion.ApiVersion >= 9 {
		if tc, err := readCompactArrayLength(reader); err != nil {
			return false, nil, err
		} else {
			topicsCount = tc
		}
	} else {
		if err = binary.Read(reader, binary.BigEndian, &topicsCount); err != nil {
			return false, nil, err
		}
	}
	logrus.Debugf("Topics count: %d", topicsCount)

	topicNames = make([]string, 0, topicsCount)

	for i := int32(0); i < topicsCount; i++ {
		var currentTopicName string
		if requestKeyVersion.ApiVersion >= 9 {
			if currentTopicName, err = readCompactString(reader); err != nil {
				return false, nil, err
			}
			logrus.Debugf("Current topic name: %s", currentTopicName)
			// Read partition data
			partitionCount, err := readCompactArrayLength(reader)
			if err != nil {
				logrus.Errorf("Failed to read partition count for topic %s: %v", currentTopicName, err)
				return false, nil, err
			}

			for j := int32(0); j < partitionCount; j++ {
				// Read partition index
				if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
					logrus.Errorf("Failed to read partition index: %v", err)
					return false, nil, err
				}

				// Read records (COMPACT_RECORDS)
				recordsLength, err := readUVarint(reader)
				if err != nil {
					logrus.Debugf("Records length: %d", recordsLength)
					return false, nil, err
				}
				if _, err := io.CopyN(io.Discard, reader, int64(recordsLength)); err != nil {
					logrus.Errorf("Failed to read records: %v", err)
					return false, nil, err
				}

				// Read tagged fields for partition
				if err = readTaggedFields(reader); err != nil {
					logrus.Errorf("Failed to read tagged fields for partition: %v", err)
					return false, nil, err
				}
			}

			// Log for debugging
			logrus.Debugf("Processed topic %s", currentTopicName)

			// Read tagged fields for topic
			if err = readTaggedFields(reader); err != nil {
				logrus.Errorf("Failed to read tagged fields for topic: %v", err)
				return false, nil, err
			}

		} else {
			if currentTopicName, err = readString(reader); err != nil {
				return false, nil, err
			}
			// Similar handling for older versions...
		}

		topicNames = append(topicNames, currentTopicName)
	}

	return !ctx.producerAcks0Disabled && acks != 0, topicNames, nil
}

func (handler *DefaultRequestHandler) handleFetch(
	reader io.Reader,
	requestKeyVersion *protocol.RequestKeyVersion,
) (bool, []string, error) {
	var (
		topicNames []string
		err        error
	)

	// Read initial fields based on version
	// Read replica_id (INT32) for versions <=14 and version 17
	if requestKeyVersion.ApiVersion <= 14 || requestKeyVersion.ApiVersion == 17 {
		if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
			return false, nil, err
		}
	}

	// Read max_wait_ms (INT32)
	if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
		return false, nil, err
	}

	// Read min_bytes (INT32)
	if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
		return false, nil, err
	}

	// Read max_bytes (INT32) if version >= 3
	if requestKeyVersion.ApiVersion >= 3 {
		if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
			return false, nil, err
		}
	}

	// Read isolation_level (INT8) if version >= 4
	if requestKeyVersion.ApiVersion >= 4 {
		if err = binary.Read(reader, binary.BigEndian, new(int8)); err != nil {
			return false, nil, err
		}
	}

	// Read session_id (INT32) and session_epoch (INT32) if version >= 7
	if requestKeyVersion.ApiVersion >= 7 {
		if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
			return false, nil, err
		}
		if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
			return false, nil, err
		}
	}

	// Read topics and collect topic names
	topicNames, err = readFetchTopics(reader, requestKeyVersion)
	if err != nil {
		return false, nil, err
	}

	// Skip forgotten_topics_data if version >= 7
	if requestKeyVersion.ApiVersion >= 7 {
		if err = skipForgottenTopicsData(reader, requestKeyVersion); err != nil {
			return false, nil, err
		}
	}

	// Read rack_id if version >= 11
	if requestKeyVersion.ApiVersion >= 11 {
		if requestKeyVersion.ApiVersion >= 12 {
			// Read rack_id (COMPACT_STRING)
			if _, err = readCompactString(reader); err != nil {
				return false, nil, err
			}
			// Read tagged fields
			if err = readTaggedFields(reader); err != nil {
				return false, nil, err
			}
		} else {
			// Read rack_id (STRING)
			if _, err = readString(reader); err != nil {
				return false, nil, err
			}
		}
	}

	// Read tagged fields for request if version >= 12
	if requestKeyVersion.ApiVersion >= 12 {
		if err = readTaggedFields(reader); err != nil {
			return false, nil, err
		}
	}

	return true, topicNames, nil
}

func readFetchTopics(reader io.Reader, requestKeyVersion *protocol.RequestKeyVersion) ([]string, error) {
	var (
		topicNames []string
		err        error
	)

	if requestKeyVersion.ApiVersion >= 12 {
		// Topics are COMPACT_ARRAY
		topicsCount, err := readCompactArrayLength(reader)
		if err != nil {
			return nil, err
		}
		topicNames = make([]string, 0, topicsCount)

		for i := int32(0); i < topicsCount; i++ {
			if requestKeyVersion.ApiVersion >= 13 {
				// Skip topic_id (UUID)
				if _, err := io.CopyN(io.Discard, reader, 16); err != nil {
					return nil, err
				}
			} else {
				// Read topic name (COMPACT_STRING)
				var topicName string
				if topicName, err = readCompactString(reader); err != nil {
					return nil, err
				}
				topicNames = append(topicNames, topicName)
			}

			// Read partitions
			if err = skipPartitions(reader, requestKeyVersion); err != nil {
				return nil, err
			}

			// Read tagged fields for topic
			if err = readTaggedFields(reader); err != nil {
				return nil, err
			}
		}
	} else {
		// Topics are ARRAY of STRING
		var topicsCount int32
		if err = binary.Read(reader, binary.BigEndian, &topicsCount); err != nil {
			return nil, err
		}
		topicNames = make([]string, 0, topicsCount)

		for i := int32(0); i < topicsCount; i++ {
			// Read topic name (STRING)
			var topicName string
			if topicName, err = readString(reader); err != nil {
				return nil, err
			}
			topicNames = append(topicNames, topicName)

			// Read partitions
			if err = skipPartitions(reader, requestKeyVersion); err != nil {
				return nil, err
			}
		}
	}

	return topicNames, nil
}

func skipPartitions(reader io.Reader, requestKeyVersion *protocol.RequestKeyVersion) error {
	var partitionsCount int32
	var err error

	if requestKeyVersion.ApiVersion >= 12 {
		// Read partitions (COMPACT_ARRAY)
		if partitionsCount, err = readCompactArrayLength(reader); err != nil {
			return err
		}
	} else {
		// Read partitions (ARRAY)
		if err = binary.Read(reader, binary.BigEndian, &partitionsCount); err != nil {
			return err
		}
	}

	for j := int32(0); j < partitionsCount; j++ {
		// Skip partition index (INT32)
		if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
			return err
		}

		// Read current_leader_epoch (INT32) if version >= 9
		if requestKeyVersion.ApiVersion >= 9 {
			if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
				return err
			}
		}

		// Skip fetch_offset (INT64)
		if err = binary.Read(reader, binary.BigEndian, new(int64)); err != nil {
			return err
		}

		// Read last_fetched_epoch (INT32) if version >= 12
		if requestKeyVersion.ApiVersion >= 12 {
			if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
				return err
			}
		}

		// Read log_start_offset (INT64) if version >= 5
		if requestKeyVersion.ApiVersion >= 5 {
			if err = binary.Read(reader, binary.BigEndian, new(int64)); err != nil {
				return err
			}
		}

		// Skip partition_max_bytes (INT32)
		if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
			return err
		}

		// Read tagged fields if version >= 12
		if requestKeyVersion.ApiVersion >= 12 {
			if err = readTaggedFields(reader); err != nil {
				return err
			}
		}
	}

	return nil
}

func skipForgottenTopicsData(reader io.Reader, requestKeyVersion *protocol.RequestKeyVersion) error {
	var err error
	var forgottenTopicsCount int32

	if requestKeyVersion.ApiVersion >= 12 {
		// Read forgotten_topics_data (COMPACT_ARRAY)
		if forgottenTopicsCount, err = readCompactArrayLength(reader); err != nil {
			return err
		}
	} else {
		// Read forgotten_topics_data (ARRAY)
		if err = binary.Read(reader, binary.BigEndian, &forgottenTopicsCount); err != nil {
			return err
		}
	}

	for i := int32(0); i < forgottenTopicsCount; i++ {
		if requestKeyVersion.ApiVersion >= 13 {
			// Skip topic_id (UUID)
			if _, err := io.CopyN(io.Discard, reader, 16); err != nil {
				return err
			}
		} else if requestKeyVersion.ApiVersion >= 12 {
			// Skip topic name (COMPACT_STRING)
			if _, err = readCompactString(reader); err != nil {
				return err
			}
		} else {
			// Skip topic name (STRING)
			if _, err = readString(reader); err != nil {
				return err
			}
		}

		// Skip partitions
		if err = skipForgottenPartitions(reader, requestKeyVersion); err != nil {
			return err
		}

		// Read tagged fields if version >= 12
		if requestKeyVersion.ApiVersion >= 12 {
			if err = readTaggedFields(reader); err != nil {
				return err
			}
		}
	}

	return nil
}

func skipForgottenPartitions(reader io.Reader, requestKeyVersion *protocol.RequestKeyVersion) error {
	var partitionsCount int32
	var err error

	if requestKeyVersion.ApiVersion >= 12 {
		// Read partitions (COMPACT_ARRAY)
		if partitionsCount, err = readCompactArrayLength(reader); err != nil {
			return err
		}
	} else {
		// Read partitions (ARRAY)
		if err = binary.Read(reader, binary.BigEndian, &partitionsCount); err != nil {
			return err
		}
	}

	// Skip partitions data
	if _, err = io.CopyN(io.Discard, reader, int64(partitionsCount*4)); err != nil {
		return err
	}

	return nil
}

func (handler *DefaultRequestHandler) handleListOffsets(
	reader io.Reader,
	requestKeyVersion *protocol.RequestKeyVersion,
) (bool, []string, error) {
	var (
		topicNames []string
		err        error
	)

	// Read ReplicaID (INT32)
	if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
		return false, nil, err
	}

	if requestKeyVersion.ApiVersion >= 2 {
		// Read IsolationLevel (INT8)
		if err = binary.Read(reader, binary.BigEndian, new(int8)); err != nil {
			return false, nil, err
		}
	}

	if requestKeyVersion.ApiVersion >= 6 {
		// Read Topics (COMPACT_ARRAY)
		topicsCount, err := readCompactArrayLength(reader)
		if err != nil {
			return false, nil, err
		}

		topicNames = make([]string, 0, topicsCount)

		for i := int32(0); i < topicsCount; i++ {
			// Read TopicName (COMPACT_STRING)
			topicName, err := readCompactString(reader)
			if err != nil {
				return false, nil, err
			}
			topicNames = append(topicNames, topicName)

			// Read Partitions (COMPACT_ARRAY)
			partitionCount, err := readCompactArrayLength(reader)
			if err != nil {
				return false, nil, err
			}

			// Skip partitions
			for j := int32(0); j < partitionCount; j++ {
				// Read PartitionIndex (INT32)
				if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
					return false, nil, err
				}

				// Read CurrentLeaderEpoch (INT32) if version >= 4
				if requestKeyVersion.ApiVersion >= 4 {
					if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
						return false, nil, err
					}
				}

				// Read Timestamp (INT64)
				if err = binary.Read(reader, binary.BigEndian, new(int64)); err != nil {
					return false, nil, err
				}

				// Read TaggedFields (TAG_BUFFER)
				if err = readTaggedFields(reader); err != nil {
					return false, nil, err
				}
			}

			// Read TaggedFields for Topic (TAG_BUFFER)
			if err = readTaggedFields(reader); err != nil {
				return false, nil, err
			}
		}

		// Read TaggedFields for Request (TAG_BUFFER)
		if err = readTaggedFields(reader); err != nil {
			return false, nil, err
		}

	} else {
		// Read TopicsCount (INT32)
		var topicsCount int32
		if err = binary.Read(reader, binary.BigEndian, &topicsCount); err != nil {
			return false, nil, err
		}

		topicNames = make([]string, 0, topicsCount)

		for i := int32(0); i < topicsCount; i++ {
			// Read TopicName (STRING)
			topicName, err := readString(reader)
			if err != nil {
				return false, nil, err
			}
			topicNames = append(topicNames, topicName)

			// Read Partitions (ARRAY)
			var partitionCount int32
			if err = binary.Read(reader, binary.BigEndian, &partitionCount); err != nil {
				return false, nil, err
			}

			// Skip partitions
			for j := int32(0); j < partitionCount; j++ {
				// Read PartitionIndex (INT32)
				if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
					return false, nil, err
				}

				// Read Timestamp (INT64)
				if err = binary.Read(reader, binary.BigEndian, new(int64)); err != nil {
					return false, nil, err
				}

				if requestKeyVersion.ApiVersion == 0 {
					// Read MaxNumOffsets (INT32)
					if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
						return false, nil, err
					}
				}
			}
		}
	}

	return true, topicNames, nil
}

func (handler *DefaultRequestHandler) handleCreateTopics(
	reader io.Reader,
	requestKeyVersion *protocol.RequestKeyVersion,
) (bool, []string, error) {
	var (
		topicNames []string
		err        error
	)

	if requestKeyVersion.ApiVersion >= 5 {
		// Read topics as COMPACT_ARRAY
		topicsCount, err := readCompactArrayLength(reader)
		if err != nil {
			return false, nil, err
		}

		topicNames = make([]string, 0, topicsCount)

		for i := int32(0); i < topicsCount; i++ {
			// Read TopicName (COMPACT_STRING)
			var topicName string
			if topicName, err = readCompactString(reader); err != nil {
				return false, nil, err
			}

			topicNames = append(topicNames, topicName)

			// Skip num_partitions (INT32)
			if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
				return false, nil, err
			}

			// Skip replication_factor (INT16)
			if err = binary.Read(reader, binary.BigEndian, new(int16)); err != nil {
				return false, nil, err
			}

			// Skip assignments
			assignmentsCount, err := readCompactArrayLength(reader)
			if err != nil {
				return false, nil, err
			}
			for j := int32(0); j < assignmentsCount; j++ {
				// Skip partition_index (INT32)
				if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
					return false, nil, err
				}
				// Skip broker_ids (COMPACT_ARRAY of INT32)
				brokerIdsCount, err := readCompactArrayLength(reader)
				if err != nil {
					return false, nil, err
				}
				if _, err = io.CopyN(io.Discard, reader, int64(brokerIdsCount*4)); err != nil {
					return false, nil, err
				}
				// Read tagged fields for assignment
				if err = readTaggedFields(reader); err != nil {
					return false, nil, err
				}
			}

			// Skip configs
			configsCount, err := readCompactArrayLength(reader)
			if err != nil {
				return false, nil, err
			}
			for j := int32(0); j < configsCount; j++ {
				// Skip name (COMPACT_STRING)
				if _, err = readCompactString(reader); err != nil {
					return false, nil, err
				}
				// Skip value (COMPACT_NULLABLE_STRING)
				if _, err = readCompactNullableString(reader); err != nil {
					return false, nil, err
				}
				// Read tagged fields for config
				if err = readTaggedFields(reader); err != nil {
					return false, nil, err
				}
			}

			// Read tagged fields for topic
			if err = readTaggedFields(reader); err != nil {
				return false, nil, err
			}
		}

		// Read timeout_ms (INT32)
		if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
			return false, nil, err
		}

		// Read validate_only (BOOLEAN)
		if err = binary.Read(reader, binary.BigEndian, new(bool)); err != nil {
			return false, nil, err
		}

		// Read tagged fields for request
		if err = readTaggedFields(reader); err != nil {
			return false, nil, err
		}
	} else {
		// For versions <5, read topics as ARRAY
		var topicsCount int32
		if err = binary.Read(reader, binary.BigEndian, &topicsCount); err != nil {
			return false, nil, err
		}

		topicNames = make([]string, 0, topicsCount)

		for i := int32(0); i < topicsCount; i++ {
			// Read TopicName (STRING)
			var topicName string
			if topicName, err = readString(reader); err != nil {
				return false, nil, err
			}

			topicNames = append(topicNames, topicName)

			// Skip num_partitions (INT32)
			if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
				return false, nil, err
			}

			// Skip replication_factor (INT16)
			if err = binary.Read(reader, binary.BigEndian, new(int16)); err != nil {
				return false, nil, err
			}

			// Skip assignments
			var assignmentsCount int32
			if err = binary.Read(reader, binary.BigEndian, &assignmentsCount); err != nil {
				return false, nil, err
			}
			for j := int32(0); j < assignmentsCount; j++ {
				// Skip partition_index (INT32)
				if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
					return false, nil, err
				}
				// Skip broker_ids (ARRAY of INT32)
				var brokerIdsCount int32
				if err = binary.Read(reader, binary.BigEndian, &brokerIdsCount); err != nil {
					return false, nil, err
				}
				if _, err = io.CopyN(io.Discard, reader, int64(brokerIdsCount*4)); err != nil {
					return false, nil, err
				}
			}

			// Skip configs
			var configsCount int32
			if err = binary.Read(reader, binary.BigEndian, &configsCount); err != nil {
				return false, nil, err
			}
			for j := int32(0); j < configsCount; j++ {
				// Skip name (STRING)
				if _, err = readString(reader); err != nil {
					return false, nil, err
				}
				// Skip value (NULLABLE_STRING)
				if _, err = readNullableString(reader); err != nil {
					return false, nil, err
				}
			}
		}

		// Read timeout_ms (INT32)
		if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
			return false, nil, err
		}

		// Read validate_only (BOOLEAN) for versions >= 1
		if requestKeyVersion.ApiVersion >= 1 {
			if err = binary.Read(reader, binary.BigEndian, new(bool)); err != nil {
				return false, nil, err
			}
		}
	}

	return true, topicNames, nil
}

func (handler *DefaultRequestHandler) handleDeleteTopics(
	reader io.Reader,
	requestKeyVersion *protocol.RequestKeyVersion,
) (bool, []string, error) {
	var (
		topicNames []string
		err        error
	)

	if requestKeyVersion.ApiVersion >= 6 {
		// Read topics as COMPACT_ARRAY
		topicsCount, err := readCompactArrayLength(reader)
		if err != nil {
			return false, nil, err
		}

		if topicsCount > 0 {
			for i := int32(0); i < topicsCount; i++ {
				// Read topic name (COMPACT_NULLABLE_STRING)
				topicName, err := readCompactNullableString(reader)
				if err != nil {
					return false, nil, err
				}
				topicNames = append(topicNames, topicName)

				// Skip topic_id UUID (16 bytes)
				uuidBytes := make([]byte, 16)
				if _, err := io.ReadFull(reader, uuidBytes); err != nil {
					return false, nil, err
				}

				// Read tagged fields for topic
				if err = readTaggedFields(reader); err != nil {
					return false, nil, err
				}
			}
		}

		// Read timeout_ms (INT32)
		if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
			return false, nil, err
		}

		// Read tagged fields for request
		if err = readTaggedFields(reader); err != nil {
			return false, nil, err
		}

	} else if requestKeyVersion.ApiVersion >= 4 {
		// Read topics as COMPACT_ARRAY
		topicsCount, err := readCompactArrayLength(reader)
		if err != nil {
			return false, nil, err
		}

		logrus.Debugf("Topics count: %d", topicsCount)

		if topicsCount > 0 {
			for i := int32(0); i < topicsCount; i++ {
				// Read TopicName (COMPACT_STRING)
				topicName, err := readCompactNullableString(reader)
				if err != nil {
					return false, nil, err
				}
				topicNames = append(topicNames, topicName)
			}
		}

		// Read timeout_ms (INT32)
		if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
			return false, nil, err
		}

		// Read tagged fields
		if err = readTaggedFields(reader); err != nil {
			return false, nil, err
		}

	} else {
		// Versions 0-3 use array of STRINGs
		var topicsCount int32
		if err = binary.Read(reader, binary.BigEndian, &topicsCount); err != nil {
			return false, nil, err
		}

		if topicsCount > 0 {
			for i := int32(0); i < topicsCount; i++ {
				// Read TopicName (STRING)
				topicName, err := readString(reader)
				if err != nil {
					return false, nil, err
				}
				topicNames = append(topicNames, topicName)
			}
		}

		// Read timeout_ms (INT32)
		if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
			return false, nil, err
		}
	}

	return true, topicNames, nil
}

func (handler *DefaultRequestHandler) handleDeleteRecords(
	reader io.Reader,
	requestKeyVersion *protocol.RequestKeyVersion,
) (bool, []string, error) {
	var (
		topicNames []string
		err        error
	)

	if requestKeyVersion.ApiVersion >= 2 {
		// Read topics as COMPACT_ARRAY
		topicsCount, err := readCompactArrayLength(reader)
		if err != nil {
			return false, nil, err
		}

		topicNames = make([]string, 0, topicsCount)

		for i := int32(0); i < topicsCount; i++ {
			// Read topic name (COMPACT_STRING)
			var topicName string
			if topicName, err = readCompactString(reader); err != nil {
				return false, nil, err
			}
			topicNames = append(topicNames, topicName)

			// Read partitions array
			partitionsCount, err := readCompactArrayLength(reader)
			if err != nil {
				return false, nil, err
			}

			// Skip partition data
			for j := int32(0); j < partitionsCount; j++ {
				// Skip partition_index (INT32)
				if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
					return false, nil, err
				}

				// Skip offset (INT64)
				if err = binary.Read(reader, binary.BigEndian, new(int64)); err != nil {
					return false, nil, err
				}

				// Read tagged fields for partition
				if err = readTaggedFields(reader); err != nil {
					return false, nil, err
				}
			}

			// Read tagged fields for topic
			if err = readTaggedFields(reader); err != nil {
				return false, nil, err
			}
		}

		// Read timeout_ms (INT32)
		if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
			return false, nil, err
		}

		// Read tagged fields for request
		if err = readTaggedFields(reader); err != nil {
			return false, nil, err
		}

	} else {
		// Read topics array (normal array for versions 0-1)
		var topicsCount int32
		if err = binary.Read(reader, binary.BigEndian, &topicsCount); err != nil {
			return false, nil, err
		}

		topicNames = make([]string, 0, topicsCount)

		for i := int32(0); i < topicsCount; i++ {
			// Read topic name (STRING)
			var topicName string
			if topicName, err = readString(reader); err != nil {
				return false, nil, err
			}
			topicNames = append(topicNames, topicName)

			// Read partitions array
			var partitionsCount int32
			if err = binary.Read(reader, binary.BigEndian, &partitionsCount); err != nil {
				return false, nil, err
			}

			// Skip partition data
			for j := int32(0); j < partitionsCount; j++ {
				// Skip partition_index (INT32)
				if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
					return false, nil, err
				}

				// Skip offset (INT64)
				if err = binary.Read(reader, binary.BigEndian, new(int64)); err != nil {
					return false, nil, err
				}
			}
		}

		// Read timeout_ms (INT32)
		if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
			return false, nil, err
		}
	}

	return true, topicNames, nil
}

func (handler *DefaultRequestHandler) handleAddPartitionsToTxn(
	reader io.Reader,
	requestKeyVersion *protocol.RequestKeyVersion,
) (bool, []string, error) {
	var (
		topicNames []string
		err        error
	)

	if requestKeyVersion.ApiVersion >= 4 {
		// Read transactions array as COMPACT_ARRAY
		transactionsCount, err := readCompactArrayLength(reader)
		if err != nil {
			return false, nil, err
		}

		topicNames = make([]string, 0)

		for i := int32(0); i < transactionsCount; i++ {
			// Read transactional_id (COMPACT_STRING)
			if _, err = readCompactString(reader); err != nil {
				return false, nil, err
			}

			// Skip producer_id (INT64)
			if err = binary.Read(reader, binary.BigEndian, new(int64)); err != nil {
				return false, nil, err
			}

			// Skip producer_epoch (INT16)
			if err = binary.Read(reader, binary.BigEndian, new(int16)); err != nil {
				return false, nil, err
			}

			// Skip verify_only (BOOLEAN)
			if err = binary.Read(reader, binary.BigEndian, new(bool)); err != nil {
				return false, nil, err
			}

			// Read topics array
			topicsCount, err := readCompactArrayLength(reader)
			if err != nil {
				return false, nil, err
			}

			for j := int32(0); j < topicsCount; j++ {
				// Read topic name
				topicName, err := readCompactString(reader)
				if err != nil {
					return false, nil, err
				}
				topicNames = append(topicNames, topicName)

				// Skip partitions array
				partitionsCount, err := readCompactArrayLength(reader)
				if err != nil {
					return false, nil, err
				}

				// Skip partition indexes
				if _, err = io.CopyN(io.Discard, reader, int64(partitionsCount*4)); err != nil {
					return false, nil, err
				}

				// Read tagged fields for partitions
				if err = readTaggedFields(reader); err != nil {
					return false, nil, err
				}
			}

			// Read tagged fields for topics
			if err = readTaggedFields(reader); err != nil {
				return false, nil, err
			}
		}

		// Read tagged fields for request
		if err = readTaggedFields(reader); err != nil {
			return false, nil, err
		}

	} else {
		// Read transactional_id
		if requestKeyVersion.ApiVersion >= 3 {
			if _, err = readCompactString(reader); err != nil {
				return false, nil, err
			}
		} else {
			if _, err = readString(reader); err != nil {
				return false, nil, err
			}
		}

		// Skip producer_id (INT64)
		if err = binary.Read(reader, binary.BigEndian, new(int64)); err != nil {
			return false, nil, err
		}

		// Skip producer_epoch (INT16)
		if err = binary.Read(reader, binary.BigEndian, new(int16)); err != nil {
			return false, nil, err
		}

		// Read topics array
		var topicsCount int32
		if requestKeyVersion.ApiVersion >= 3 {
			if topicsCount, err = readCompactArrayLength(reader); err != nil {
				return false, nil, err
			}
		} else {
			if err = binary.Read(reader, binary.BigEndian, &topicsCount); err != nil {
				return false, nil, err
			}
		}

		topicNames = make([]string, 0, topicsCount)

		for i := int32(0); i < topicsCount; i++ {
			// Read topic name
			var topicName string
			if requestKeyVersion.ApiVersion >= 3 {
				if topicName, err = readCompactString(reader); err != nil {
					return false, nil, err
				}
			} else {
				if topicName, err = readString(reader); err != nil {
					return false, nil, err
				}
			}
			topicNames = append(topicNames, topicName)

			// Skip partitions array
			var partitionsCount int32
			if err = binary.Read(reader, binary.BigEndian, &partitionsCount); err != nil {
				return false, nil, err
			}

			// Skip partition indexes
			if _, err = io.CopyN(io.Discard, reader, int64(partitionsCount*4)); err != nil {
				return false, nil, err
			}

			if requestKeyVersion.ApiVersion >= 3 {
				// Read tagged fields for topic
				if err = readTaggedFields(reader); err != nil {
					return false, nil, err
				}
			}
		}

		if requestKeyVersion.ApiVersion >= 3 {
			// Read tagged fields for request
			if err = readTaggedFields(reader); err != nil {
				return false, nil, err
			}
		}
	}

	return true, topicNames, nil
}

func (handler *DefaultRequestHandler) handleCreatePartitions(
	reader io.Reader,
	requestKeyVersion *protocol.RequestKeyVersion,
) (bool, []string, error) {
	var (
		topicNames []string
		err        error
	)

	if requestKeyVersion.ApiVersion >= 2 {
		// Read topics array (COMPACT_ARRAY)
		topicsCount, err := readCompactArrayLength(reader)
		if err != nil {
			return false, nil, err
		}

		topicNames = make([]string, 0, topicsCount)

		for i := int32(0); i < topicsCount; i++ {
			// Read topic name (COMPACT_STRING)
			var topicName string
			if topicName, err = readCompactString(reader); err != nil {
				return false, nil, err
			}
			topicNames = append(topicNames, topicName)

			// Skip count (INT32)
			if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
				return false, nil, err
			}

			// Read assignments array
			assignmentsCount, err := readCompactArrayLength(reader)
			if err != nil {
				return false, nil, err
			}

			// Skip assignments data
			for j := int32(0); j < assignmentsCount; j++ {
				// Read broker_ids array
				brokerIdsCount, err := readCompactArrayLength(reader)
				if err != nil {
					return false, nil, err
				}

				// Skip broker IDs
				if _, err = io.CopyN(io.Discard, reader, int64(brokerIdsCount*4)); err != nil {
					return false, nil, err
				}

				// Read tagged fields for broker_ids array
				if err = readTaggedFields(reader); err != nil {
					return false, nil, err
				}
			}

			// Read tagged fields for topic
			if err = readTaggedFields(reader); err != nil {
				return false, nil, err
			}
		}

		// Skip timeout_ms (INT32)
		if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
			return false, nil, err
		}

		// Skip validate_only (BOOLEAN)
		if err = binary.Read(reader, binary.BigEndian, new(bool)); err != nil {
			return false, nil, err
		}

		// Read tagged fields for request
		if err = readTaggedFields(reader); err != nil {
			return false, nil, err
		}

	} else {
		// Read topics array
		var topicsCount int32
		if err = binary.Read(reader, binary.BigEndian, &topicsCount); err != nil {
			return false, nil, err
		}

		topicNames = make([]string, 0, topicsCount)

		for i := int32(0); i < topicsCount; i++ {
			// Read topic name (STRING)
			var topicName string
			if topicName, err = readString(reader); err != nil {
				return false, nil, err
			}
			topicNames = append(topicNames, topicName)

			// Skip count (INT32)
			if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
				return false, nil, err
			}

			// Read assignments array
			var assignmentsCount int32
			if err = binary.Read(reader, binary.BigEndian, &assignmentsCount); err != nil {
				return false, nil, err
			}

			// Skip assignments data
			for j := int32(0); j < assignmentsCount; j++ {
				// Read broker_ids array
				var brokerIdsCount int32
				if err = binary.Read(reader, binary.BigEndian, &brokerIdsCount); err != nil {
					return false, nil, err
				}

				// Skip broker IDs
				if _, err = io.CopyN(io.Discard, reader, int64(brokerIdsCount*4)); err != nil {
					return false, nil, err
				}
			}
		}

		// Skip timeout_ms (INT32)
		if err = binary.Read(reader, binary.BigEndian, new(int32)); err != nil {
			return false, nil, err
		}

		// Skip validate_only (BOOLEAN)
		if err = binary.Read(reader, binary.BigEndian, new(bool)); err != nil {
			return false, nil, err
		}
	}

	return true, topicNames, nil
}

func readString(reader io.Reader) (string, error) {
	var length int16
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return "", err
	}
	if length < 0 {
		return "", fmt.Errorf("Invalid string length %d", length)
	} else if length == 0 {
		return "", nil
	} else {
		strBytes := make([]byte, length)
		if _, err := io.ReadFull(reader, strBytes); err != nil {
			return "", err
		}
		return string(strBytes), nil
	}
}

func readCompactNullableString(reader io.Reader) (string, error) {
	// Read the length as an unsigned VarInt
	length, err := readUVarint(reader)
	if err != nil {
		return "", err
	}

	if length == 0 {
		// Null string
		return "", nil
	}

	strLen := length - 1

	// Ensure the string length is valid
	if strLen < 0 {
		return "", errors.New("invalid string length")
	}

	// Read the string bytes
	strBytes := make([]byte, strLen)
	if _, err := io.ReadFull(reader, strBytes); err != nil {
		return "", err
	}

	return string(strBytes), nil
}

func readUVarint(reader io.Reader) (uint64, error) {
	var value uint64
	var shift uint
	for {
		var b [1]byte
		if _, err := io.ReadFull(reader, b[:]); err != nil {
			return 0, err
		}
		value |= uint64(b[0]&0x7F) << shift
		if (b[0] & 0x80) == 0 {
			break
		}
		shift += 7
		if shift > 63 {
			return 0, fmt.Errorf("varint too long")
		}
	}
	return value, nil
}

func readCompactString(reader io.Reader) (string, error) {
	length, err := readUVarint(reader)
	if err != nil {
		return "", err
	}
	if length == 0 {
		return "", nil
	}
	length-- // Adjust for compact encoding (length includes one extra byte)
	buf := make([]byte, length)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readCompactArrayLength(reader io.Reader) (int32, error) {
	length, err := readUVarint(reader)
	if err != nil {
		return 0, err
	}
	if length == 0 {
		return 0, nil
	}
	return int32(length - 1), nil // Adjust for compact encoding
}

func readTaggedFields(reader io.Reader) error {
	numTags, err := readUVarint(reader)
	if err != nil {
		return err
	}
	for i := uint64(0); i < numTags; i++ {
		// Read tag (UVarint)
		_, err := readUVarint(reader)
		if err != nil {
			return err
		}
		// Read size (UVarint)
		size, err := readUVarint(reader)
		if err != nil {
			return err
		}
		// Skip over the tag data
		if _, err := io.CopyN(io.Discard, reader, int64(size)); err != nil {
			return err
		}
	}
	return nil
}

func readNullableString(reader io.Reader) (string, error) {
	var length int16
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return "", err
	}
	if length < 0 {
		// Null string
		return "", nil
	} else if length == 0 {
		return "", nil
	} else {
		strBytes := make([]byte, length)
		if _, err := io.ReadFull(reader, strBytes); err != nil {
			return "", err
		}
		return string(strBytes), nil
	}
}

func (handler *DefaultResponseHandler) handleResponse(dst DeadlineWriter, src DeadlineReader, ctx *ResponsesLoopContext) (readErr bool, err error) {
	//logrus.Println("Await Kafka response")

	// waiting for first bytes or EOF - reset deadlines
	if err = src.SetReadDeadline(time.Time{}); err != nil {
		return true, err
	}
	if err = dst.SetWriteDeadline(time.Time{}); err != nil {
		return true, err
	}

	responseHeaderBuf := make([]byte, 8) // Size => int32, CorrelationId => int32
	if _, err = io.ReadFull(src, responseHeaderBuf); err != nil {
		return true, err
	}

	var responseHeader protocol.ResponseHeader
	if err = protocol.Decode(responseHeaderBuf, &responseHeader); err != nil {
		return true, err
	}

	// Read the inFlightRequests channel after header is read. Otherwise the channel would block and socket EOF from remote would not be received.
	requestKeyVersion, err := receiveRequestKeyVersion(ctx.openRequestsChannel, openRequestReceiveTimeout)
	if err != nil {
		return true, err
	}
	proxyResponsesBytes.WithLabelValues(ctx.brokerAddress).Add(float64(responseHeader.Length + 4))
	logrus.Debugf("Kafka response operation: %s (key: %v, version: %v, length: %v)",
		getOperationName(requestKeyVersion.ApiKey),
		requestKeyVersion.ApiKey,
		requestKeyVersion.ApiVersion,
		responseHeader.Length)

	responseDeadline := time.Now().Add(ctx.timeout)
	err = dst.SetWriteDeadline(responseDeadline)
	if err != nil {
		return false, err
	}
	err = src.SetReadDeadline(responseDeadline)
	if err != nil {
		return true, err
	}
	responseHeaderTaggedFields, err := protocol.NewResponseHeaderTaggedFields(requestKeyVersion)
	if err != nil {
		return true, err
	}
	unknownTaggedFields, err := responseHeaderTaggedFields.MaybeRead(src)
	if err != nil {
		return true, err
	}
	readResponsesHeaderLength := int32(4 + len(unknownTaggedFields)) // 4 = Length + CorrelationID

	responseModifier, err := protocol.GetResponseModifier(requestKeyVersion.ApiKey, requestKeyVersion.ApiVersion, ctx.netAddressMappingFunc, ctx.acl)
	if err != nil {
		return true, err
	}

	// TODO: implement filtering of topics in Metadata and ListTopics API Keys
	if responseModifier != nil {
		if responseHeader.Length > protocol.MaxResponseSize {
			return true, protocol.PacketDecodingError{Info: fmt.Sprintf("message of length %d too large", responseHeader.Length)}
		}
		resp := make([]byte, int(responseHeader.Length-readResponsesHeaderLength))
		if _, err = io.ReadFull(src, resp); err != nil {
			return true, err
		}
		newResponseBuf, err := responseModifier.Apply(resp)
		if err != nil {
			return true, err
		}
		// add 4 bytes (CorrelationId) to the length
		newHeaderBuf, err := protocol.Encode(&protocol.ResponseHeader{Length: int32(len(newResponseBuf) + int(readResponsesHeaderLength)), CorrelationID: responseHeader.CorrelationID})
		if err != nil {
			return true, err
		}
		if _, err := dst.Write(newHeaderBuf); err != nil {
			return false, err
		}
		if _, err := dst.Write(unknownTaggedFields); err != nil {
			return false, err
		}
		if _, err := dst.Write(newResponseBuf); err != nil {
			return false, err
		}
	} else {
		// write - send to local
		if _, err := dst.Write(responseHeaderBuf); err != nil {
			return false, err
		}
		if _, err := dst.Write(unknownTaggedFields); err != nil {
			return false, err
		}
		// 4 bytes were written as responseHeaderBuf (CorrelationId) + tagged fields
		if readErr, err = myCopyN(dst, src, int64(responseHeader.Length-readResponsesHeaderLength), ctx.buf); err != nil {
			return readErr, err
		}
	}
	return false, nil // continue nextResponse
}

func sendRequestKeyVersion(openRequestsChannel chan<- protocol.RequestKeyVersion, timeout time.Duration, request *protocol.RequestKeyVersion) error {
	select {
	case openRequestsChannel <- *request:
	default:
		// timer.Stop() will be invoked only after sendRequestKeyVersion is finished (not after select default) !
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case openRequestsChannel <- *request:
		case <-timer.C:
			return errors.New("open requests buffer is full")
		}
	}
	return nil
}

func receiveRequestKeyVersion(openRequestsChannel <-chan protocol.RequestKeyVersion, timeout time.Duration) (*protocol.RequestKeyVersion, error) {
	var request protocol.RequestKeyVersion
	select {
	case request = <-openRequestsChannel:
	default:
		// timer.Stop() will be invoked only after receiveRequestKeyVersion is finished (not after select default) !
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case request = <-openRequestsChannel:
		case <-timer.C:
			return nil, errors.New("open request is missing")
		}
	}
	return &request, nil
}
