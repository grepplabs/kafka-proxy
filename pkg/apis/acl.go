package apis

import (
	"context"
	"strings"
	"time"
)

// ResourceType represents the type of Kafka resource
type ResourceType string

const (
	ResourceTypeTopic           ResourceType = "TOPIC"
	ResourceTypeGroup           ResourceType = "GROUP"
	ResourceTypeCluster         ResourceType = "CLUSTER"
	ResourceTypeTransactionalID ResourceType = "TRANSACTIONAL_ID"
)

// Operation represents the type of operation allowed/denied
type Operation string

const (
	OperationRead          Operation = "READ"
	OperationWrite         Operation = "WRITE"
	OperationCreate        Operation = "CREATE"
	OperationDelete        Operation = "DELETE"
	OperationAlter         Operation = "ALTER"
	OperationDescribe      Operation = "DESCRIBE"
	OperationClusterAction Operation = "CLUSTER_ACTION"
	OperationAll           Operation = "ALL"
)

// PermissionType represents whether the ACL allows or denies access
type PermissionType string

const (
	PermissionAllow PermissionType = "ALLOW"
	PermissionDeny  PermissionType = "DENY"
)

// ACLEntry represents a single ACL rule
type ACLEntry struct {
	Principal      string         `json:"principal"`      // The user or group
	ResourceType   ResourceType   `json:"resourceType"`   // Type of resource
	ResourceName   string         `json:"resourceName"`   // Name of resource
	PatternType    string         `json:"patternType"`    // LITERAL or PREFIXED
	Operation      Operation      `json:"operation"`      // Type of operation
	PermissionType PermissionType `json:"permissionType"` // Allow or Deny
	Host           string         `json:"host"`           // Host from which access is allowed
	CreatedAt      time.Time      `json:"createdAt"`
	UpdatedAt      time.Time      `json:"updatedAt"`
}

// ACLCollection represents a collection of ACL entries
type ACLCollection struct {
	ACLs []ACLEntry
}

// ACLChecker interface for ACL plugins.
type ACLChecker interface {
	// CheckACL checks if a given request is allowed based on configured ACL rules.
	CheckACL(ctx context.Context, APIKey int, topics []string) (bool, []string, error)
}

// ACLCheckerFactory interface for creating ACL checkers.
type ACLCheckerFactory interface {
	New(ACLCollection) (ACLChecker, error)
}

// ACLDecision represents the result of ACL evaluation
type ACLDecision struct {
	Allowed bool
	Reason  string
}

// ACLRequest represents a request to check permissions
type ACLRequest struct {
	Principal    string
	ResourceType ResourceType
	ResourceName string
	Operation    Operation
	Host         string
}

// EvaluateAccess checks if the requested operation is allowed
func (ac *ACLCollection) EvaluateAccess(req ACLRequest) ACLDecision {
	// Quick check if there are no ACLs
	if len(ac.ACLs) == 0 {
		return ACLDecision{
			Allowed: false,
			Reason:  "no ACLs defined",
		}
	}

	// First pass: Look for explicit DENY rules (these take precedence)
	for _, acl := range ac.ACLs {
		if isDenyMatch(acl, req) {
			return ACLDecision{
				Allowed: false,
				Reason:  "explicitly denied by ACL",
			}
		}
	}

	// Second pass: Look for ALLOW rules
	for _, acl := range ac.ACLs {
		if isAllowMatch(acl, req) {
			return ACLDecision{
				Allowed: true,
				Reason:  "explicitly allowed by ACL",
			}
		}
	}

	// Default deny if no matching rules found
	return ACLDecision{
		Allowed: false,
		Reason:  "no matching allow rules found",
	}
}

// isDenyMatch checks if an ACL entry explicitly denies access
func isDenyMatch(acl ACLEntry, req ACLRequest) bool {
	if acl.PermissionType != PermissionDeny {
		return false
	}

	return isMatch(acl, req)
}

// isAllowMatch checks if an ACL entry allows access
func isAllowMatch(acl ACLEntry, req ACLRequest) bool {
	if acl.PermissionType != PermissionAllow {
		return false
	}

	return isMatch(acl, req)
}

// isMatch performs the actual matching logic
func isMatch(acl ACLEntry, req ACLRequest) bool {
	// Check Principal (using exact match or wildcard)
	if acl.Principal != "*" && acl.Principal != req.Principal {
		return false
	}

	// Check Resource Type
	if acl.ResourceType != req.ResourceType {
		return false
	}

	// Check Resource Name (using pattern matching)
	switch acl.PatternType {
	case "LITERAL":
		if acl.ResourceName != req.ResourceName {
			return false
		}
	case "PREFIXED":
		if !strings.HasPrefix(req.ResourceName, acl.ResourceName) {
			return false
		}
	}

	// Check Operation (including ALL operation)
	if acl.Operation != OperationAll && acl.Operation != req.Operation {
		return false
	}

	// Check Host (using exact match or wildcard)
	if acl.Host != "*" && acl.Host != req.Host {
		return false
	}

	return true
}
