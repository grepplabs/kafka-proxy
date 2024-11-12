package aclplugin

import (
	"flag"
	"fmt"
	"strings"

	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/grepplabs/kafka-proxy/pkg/registry"
)

func init() {
    registry.NewComponentInterface(new(apis.ACLCheckerFactory))
    registry.Register(new(Factory), "acl-plugin")
}

type pluginMeta struct {
    rules []string
}

type Factory struct{}

func (f *Factory) New(params []string) (apis.ACLChecker, error) {
    meta := &pluginMeta{}
    fs := flag.NewFlagSet("acl-plugin settings", flag.ContinueOnError)
    fs.Var(&stringArrayValue{&meta.rules}, "rule", "ACL rule (Operation,TopicPattern,Allow)")

    if err := fs.Parse(params); err != nil {
        return nil, err
    }

    rules, err := parseRules(meta.rules)
    if err != nil {
        return nil, err
    }

    return NewACLChecker(rules)
}

type stringArrayValue struct {
    target *[]string
}

func (s *stringArrayValue) String() string {
    return strings.Join(*s.target, ",")
}

func (s *stringArrayValue) Set(value string) error {
    *s.target = append(*s.target, value)
    return nil
}

func parseRules(ruleStrings []string) ([]apis.ACLRule, error) {
    rules := make([]apis.ACLRule, len(ruleStrings))
    for i, ruleStr := range ruleStrings {
        parts := strings.Split(ruleStr, ",")
        if len(parts) != 3 {
            return nil, fmt.Errorf("invalid rule format: %s", ruleStr)
        }
        allow := parts[2] == "true"
        rules[i] = apis.ACLRule{Operation: apis.Operation(parts[0]), Topic: parts[1], Allow: allow}
    }
    return rules, nil
}
