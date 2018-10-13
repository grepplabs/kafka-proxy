package proxy

import (
	"fmt"
	"github.com/pkg/errors"
	"regexp"
	"strings"
)

// https://tools.ietf.org/html/rfc7628#section-3.1
// https://tools.ietf.org/html/rfc5801#section-4
const (
	saslOauthSeparator = "\u0001"
	saslOauthSaslName  = "(?:[\\x01-\\x7F&&[^=,]]|=2C|=3D)+"
	saslOauthKey       = "[A-Za-z]+"
	saslOauthValue     = "[\\x21-\\x7E \t\r\n]+"
	saslOauthAuthKey   = "auth"
)

var (
	saslOauthKVPairs                      = fmt.Sprintf("(%s=%s%s)*", saslOauthKey, saslOauthValue, saslOauthSeparator)
	saslOauthAuthPattern                  = regexp.MustCompile("(?P<scheme>[\\w]+)[ ]+(?P<token>[-_.a-zA-Z0-9]+)")
	saslOauthClientInitialResponsePattern = regexp.MustCompile(fmt.Sprintf("n,(a=(?P<authzid>%s))?,%s(?P<kvpairs>%s)%s", saslOauthSaslName, saslOauthSeparator, saslOauthKVPairs, saslOauthSeparator))
)

type SaslOAuthBearer struct{}

func (p SaslOAuthBearer) GetToken(saslAuthBytes []byte) (string, error) {
	match := saslOauthClientInitialResponsePattern.FindSubmatch(saslAuthBytes)

	result := make(map[string][]byte)
	for i, name := range saslOauthClientInitialResponsePattern.SubexpNames() {
		if i != 0 && name != "" {
			result[name] = match[i]
		}
	}
	kvpairs := result["kvpairs"]
	properties := p.parseMap(string(kvpairs), "=", saslOauthSeparator)
	return p.parseToken(properties[saslOauthAuthKey])
}

func (SaslOAuthBearer) parseToken(auth string) (string, error) {
	if auth == "" {
		return "", errors.New("invalid OAUTHBEARER initial client response: 'auth' not specified")
	}
	match := saslOauthAuthPattern.FindStringSubmatch(auth)
	result := make(map[string]string)
	for i, name := range saslOauthAuthPattern.SubexpNames() {
		if i != 0 && name != "" {
			result[name] = match[i]
		}
	}
	if !strings.EqualFold(result["scheme"], "bearer") {
		return "", fmt.Errorf("invalid scheme in OAUTHBEARER initial client response: %s", result["scheme"])
	}
	token := result["token"]
	if token == "" {
		return "", errors.New("invalid OAUTHBEARER initial client response: 'token' is missing")
	}
	return token, nil
}

func (SaslOAuthBearer) parseMap(mapStr string, keyValueSeparator string, elementSeparator string) map[string]string {
	result := make(map[string]string)
	if mapStr == "" {
		return result
	}
	for _, attrval := range strings.Split(mapStr, elementSeparator) {
		kv := strings.SplitN(attrval, keyValueSeparator, 2)
		if len(kv) == 2 {
			result[kv[0]] = kv[1]
		}
	}
	return result
}
