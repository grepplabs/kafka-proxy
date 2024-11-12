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

func (a *ACLCheckerImpl) CheckACL(ctx context.Context, req *protocol.RequestKeyVersion, topic string) (bool, error) {
	op := getOperationFromKey(req.ApiKey)
	for _, rule := range a.rules {
		if (rule.Operation == apis.OperationAll || rule.Operation == op) &&
			(rule.Topic == "" || (topic != "" && rule.Re.MatchString(topic))) {
			return rule.Allow, nil
		}
	}
	return false, nil
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
