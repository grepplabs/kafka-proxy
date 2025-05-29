package aclplugin

import (
	"context"
	"regexp"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/proxy/protocol"
)

type ACLCheckerImpl struct {
	rules []apis.ACLRule
}

func NewACLChecker(rules []apis.ACLRule) (apis.ACLChecker, error) {
	compiledRules := make([]apis.ACLRule, len(rules))
	for i, rule := range rules {
		if rule.Topic != "" {
			re, err := regexp.Compile(rule.Topic)
			if err != nil {
				return nil, err
			}
			compiledRules[i] = apis.ACLRule{Operation: rule.Operation, Topic: rule.Topic, Allow: rule.Allow, Re: re}
		} else {
			compiledRules[i] = rule
		}
	}
	return &ACLCheckerImpl{rules: compiledRules}, nil
}

func (a *ACLCheckerImpl) CheckACL(ctx context.Context, req *protocol.RequestKeyVersion, topics []string) (bool, []string, error) {
	op := getOperationFromKey(req.ApiKey)
	for _, topic := range topics {
		anyMatched := false
		for _, rule := range a.rules {
			if (rule.Operation == apis.OperationAll || rule.Operation == op) &&
				(rule.Topic == "" || (topic != "" && rule.Re.MatchString(topic))) {
				anyMatched = true
				if !rule.Allow {
					return false, nil, nil
				}
			}
		}
		if !anyMatched {
			return false, nil, nil
		}
	}
	return true, nil, nil
}

func getOperationFromKey(apiKey int16) apis.Operation {
	switch apiKey {
	case 0:
		return apis.OperationProduce
	case 1:
		return apis.OperationFetch
	case 3:
		return apis.OperationMetadata
	default:
		return apis.OperationAll
	}
}
