package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"
)

var (
	regexUsername = regexp.MustCompile(`(?m:username[[:blank:]]*=[[:blank:]]*"(.*?)")`)
	regexPassword = regexp.MustCompile(`(?m:password[[:blank:]]*=[[:blank:]]*"(.*?)")`)
)

type JaasCredentials struct {
	Username string
	Password string
}

func NewJaasCredentialFromFile(filename string) (*JaasCredentials, error) {
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return NewJaasCredentials(string(bytes))
}

func NewJaasCredentials(s string) (*JaasCredentials, error) {
	username, err := getJaasAttr(regexUsername.FindAllStringSubmatch(s, -1))
	if err != nil {
		return nil, fmt.Errorf("cannot retrieve jaas username: %s", err.Error())
	}
	password, err := getJaasAttr(regexPassword.FindAllStringSubmatch(s, -1))
	if err != nil {
		return nil, fmt.Errorf("cannot retrieve jaas password: %s", err.Error())
	}
	return &JaasCredentials{Username: username, Password: password}, nil
}

func getJaasAttr(submatch [][]string) (string, error) {
	if len(submatch) == 0 {
		return "", errors.New("no entry was found")
	}
	if len(submatch) != 1 {
		return "", errors.New("multiple entries were found")
	}
	group := submatch[0]
	if len(group) != 2 {
		return "", errors.New("multiple entries were found")
	}
	attr := group[1]
	if attr == "" {
		return "", errors.New("attribute is empty")
	}

	return strings.TrimSpace(attr), nil
}
