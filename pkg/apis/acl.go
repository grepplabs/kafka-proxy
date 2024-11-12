package apis

import (
	"context"
	"regexp"

	"github.com/grepplabs/kafka-proxy/proxy/protocol"
)

// Operation represents a Kafka API operation.
type Operation string

// Define human-readable Kafka API operations.
const (
	OperationProduce  Operation = "Produce"
	OperationFetch    Operation = "Fetch"
	OperationMetadata Operation = "Metadata"
	OperationAll      Operation = "*" // Wildcard for all operations
	// Add other operations as needed...
)

// ACLRule defines a rule for access control.
type ACLRule struct {
	// Operation to match. "*" means match all operations.
	Operation Operation
	// Topic name pattern to match (regular expression). "" means match all topics
	Topic string
	Re    *regexp.Regexp
	// Allow (true) or deny (false) access.
	Allow bool
}

// ACLChecker interface for ACL plugins.
type ACLChecker interface {
	// CheckACL checks if a given request is allowed based on configured ACL rules.
	CheckACL(ctx context.Context, req *protocol.RequestKeyVersion, topic string) (bool, error)
}

// ACLCheckerFactory interface for creating ACL checkers.
type ACLCheckerFactory interface {
	New(rules []ACLRule) (ACLChecker, error)
}
