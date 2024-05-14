package protocol

import "fmt"

type RequestKeyVersion struct {
	Length     int32
	ApiKey     int16
	ApiVersion int16
}

func (r *RequestKeyVersion) decode(pd packetDecoder) (err error) {
	r.Length, err = pd.getInt32()
	if err != nil {
		return err
	}
	if r.Length <= 4 {
		return PacketDecodingError{fmt.Sprintf("message of length %d too small", r.Length)}
	}
	r.ApiKey, err = pd.getInt16()
	if err != nil {
		return err
	}
	r.ApiVersion, err = pd.getInt16()
	return err
}

// Determine response header version. Function returns -1 for unknown api key.
// See also public short responseHeaderVersion(short _version) in kafka/clients/src/generated/java/org/apache/kafka/common/message/ApiMessageType.java
func (r *RequestKeyVersion) ResponseHeaderVersion() int16 {
	switch r.ApiKey {
	case 0: // Produce
		if r.ApiVersion >= 9 {
			return 1
		} else {
			return 0
		}
	case 1: // Fetch
		if r.ApiVersion >= 12 {
			return 1
		} else {
			return 0
		}
	case 2: // ListOffset
		if r.ApiVersion >= 6 {
			return 1
		} else {
			return 0
		}
	case 3: // Metadata
		if r.ApiVersion >= 9 {
			return 1
		} else {
			return 0
		}
	case 4: // LeaderAndIsr
		if r.ApiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 5: // StopReplica
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 6: // UpdateMetadata
		if r.ApiVersion >= 6 {
			return 1
		} else {
			return 0
		}
	case 7: // ControlledShutdown
		if r.ApiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 8: // OffsetCommit
		if r.ApiVersion >= 8 {
			return 1
		} else {
			return 0
		}
	case 9: // OffsetFetch
		if r.ApiVersion >= 6 {
			return 1
		} else {
			return 0
		}
	case 10: // FindCoordinator
		if r.ApiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 11: // JoinGroup
		if r.ApiVersion >= 6 {
			return 1
		} else {
			return 0
		}
	case 12: // Heartbeat
		if r.ApiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 13: // LeaveGroup
		if r.ApiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 14: // SyncGroup
		if r.ApiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 15: // DescribeGroups
		if r.ApiVersion >= 5 {
			return 1
		} else {
			return 0
		}
	case 16: // ListGroups
		if r.ApiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 17: // SaslHandshake
		return 0
	case 18: // ApiVersions
		// ApiVersionsResponse always includes a v0 header.
		// See KIP-511 for details.
		return 0
	case 19: // CreateTopics
		if r.ApiVersion >= 5 {
			return 1
		} else {
			return 0
		}
	case 20: // DeleteTopics
		if r.ApiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 21: // DeleteRecords
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 22: // InitProducerId
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 23: // OffsetForLeaderEpoch
		if r.ApiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 24: // AddPartitionsToTxn
		if r.ApiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 25: // AddOffsetsToTxn
		if r.ApiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 26: // EndTxn
		if r.ApiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 27: // WriteTxnMarkers
		if r.ApiVersion >= 1 {
			return 1
		} else {
			return 0
		}
	case 28: // TxnOffsetCommit
		if r.ApiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 29: // DescribeAcls
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 30: // CreateAcls
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 31: // DeleteAcls
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 32: // DescribeConfigs
		if r.ApiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 33: // AlterConfigs
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 34: // AlterReplicaLogDirs
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 35: // DescribeLogDirs
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 36: // SaslAuthenticate
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 37: // CreatePartitions
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 38: // CreateDelegationToken
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 39: // RenewDelegationToken
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 40: // ExpireDelegationToken
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 41: // DescribeDelegationToken
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 42: // DeleteGroups
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 43: // ElectLeaders
		if r.ApiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 44: // IncrementalAlterConfigs
		if r.ApiVersion >= 1 {
			return 1
		} else {
			return 0
		}
	case 45: // AlterPartitionReassignments
		return 1
	case 46: // ListPartitionReassignments
		return 1
	case 47: // OffsetDelete
		return 0
	case 48: // DescribeClientQuotas
		if r.ApiVersion >= 1 {
			return 1
		} else {
			return 0
		}
	case 49: // AlterClientQuotas
		if r.ApiVersion >= 1 {
			return 1
		} else {
			return 0
		}
	case 50: // DescribeUserScramCredentials
		return 1
	case 51: // AlterUserScramCredentials
		return 1
	case 52: // Vote
		return 1
	case 53: // BeginQuorumEpoch
		return 0
	case 54: // EndQuorumEpoch
		return 0
	case 55: // DescribeQuorum
		return 1
	case 56: // DescribeQuorum
		return 1
	case 57: // UpdateFeatures
		return 1
	case 58: // Envelope
		return 1
	case 59: // FetchSnapshot
		return 1
	case 60: // DescribeCluster
		return 1
	case 61: // DescribeProducers
		return 1
	case 62: // BrokerRegistration
		return 1
	case 63: // BrokerHeartbeat
		return 1
	case 64: // UnregisterBroker
		return 1
	case 65: // DescribeTransactions
		return 1
	case 66: // ListTransactions
		return 1
	case 67: // AllocateProducerIds
		return 1
	case 68: // ConsumerGroupHeartbeat
		return 1
	case 69: // ConsumerGroupDescribe
		return 1
	case 70: // ControllerRegistration
		return 1
	case 71: // GetTelemetrySubscriptions
		return 1
	case 72: // PushTelemetry
		return 1
	case 73: // AssignReplicasToDirs
		return 1
	case 74: // ListClientMetricsResources
		return 1
	default:
		// throw new UnsupportedVersionException("Unsupported API key " + apiKey);
		return -1
	}
}
