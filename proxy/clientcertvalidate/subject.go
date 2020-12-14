package clientcertvalidate

import (
	"crypto/x509"
	"fmt"
	"regexp"
	"sort"

	"github.com/pkg/errors"
)

const (
	clientCertSubjectCommonName         = "CN"
	clientCertSubjectCountry            = "C"
	clientCertSubjectLocality           = "L"
	clientCertSubjectOrganization       = "O"
	clientCertSubjectOrganizationalUnit = "OU"
	clientCertSubjectProvince           = "S"
)

var validSubjectFields = map[string]bool{
	clientCertSubjectCommonName:         true,
	clientCertSubjectCountry:            true,
	clientCertSubjectLocality:           true,
	clientCertSubjectOrganization:       true,
	clientCertSubjectOrganizationalUnit: true,
	clientCertSubjectProvince:           true,
}

// ClientCertificateSubjectPrefixType is the subject header type.
type ClientCertificateSubjectPrefixType int

const (
	// ClientCertificateSubjectPrefixString is the s:/ header.
	ClientCertificateSubjectPrefixString ClientCertificateSubjectPrefixType = iota
	// ClientCertificateSubjectPrefixPattern is the r:/ header.
	ClientCertificateSubjectPrefixPattern
)

// ParsedSubject is the result of the subject parser Parse() operation.
// Contains operations to validate its after parse state and validate the X509 Certificate.
type ParsedSubject interface {
	KVs() KVs
	WithKVs(string, []string)
	Input() string
	RegexpKVs() RegexpKVs
	WithRegexpKVs(string, []*regexp.Regexp)
	Type() ClientCertificateSubjectPrefixType
	WithType(ClientCertificateSubjectPrefixType)
	X509Validate(*x509.Certificate) error
}

type defaultParsedSubject struct {
	input           string
	inputValuesType ClientCertificateSubjectPrefixType
	kvs             KVs
	regexpkvs       RegexpKVs
}

// DefaultParsedSubject returns an initialized instance of the default parsed subject implementation.
func DefaultParsedSubject() ParsedSubject {
	return &defaultParsedSubject{
		kvs:       make(KVs),
		regexpkvs: make(RegexpKVs),
	}
}

func (ccs *defaultParsedSubject) KVs() KVs {
	return ccs.kvs
}

func (ccs *defaultParsedSubject) Input() string {
	return ccs.input
}

func (ccs *defaultParsedSubject) RegexpKVs() RegexpKVs {
	return ccs.regexpkvs
}

func (ccs *defaultParsedSubject) Type() ClientCertificateSubjectPrefixType {
	return ccs.inputValuesType
}
func (ccs *defaultParsedSubject) WithType(input ClientCertificateSubjectPrefixType) {
	ccs.inputValuesType = input
}

func (ccs *defaultParsedSubject) WithKVs(field string, values []string) {
	ccs.kvs[field] = values
}
func (ccs *defaultParsedSubject) WithRegexpKVs(field string, values []*regexp.Regexp) {
	ccs.regexpkvs[field] = values
}

func (ccs *defaultParsedSubject) X509Validate(cert *x509.Certificate) error {
	switch ccs.inputValuesType {
	case ClientCertificateSubjectPrefixString:
		for k, v := range ccs.kvs {
			switch k {
			case clientCertSubjectCommonName:
				expected := []string{cert.Subject.CommonName}
				if err := testCertValuesSlicesStringSubject(clientCertSubjectCommonName, v, expected); err != nil {
					return err
				}
			case clientCertSubjectCountry:
				if err := testCertValuesSlicesStringSubject(clientCertSubjectCountry, v, cert.Subject.Country); err != nil {
					return err
				}
			case clientCertSubjectProvince:
				if err := testCertValuesSlicesStringSubject(clientCertSubjectProvince, v, cert.Subject.Province); err != nil {
					return err
				}
			case clientCertSubjectLocality:
				if err := testCertValuesSlicesStringSubject(clientCertSubjectLocality, v, cert.Subject.Locality); err != nil {
					return err
				}
			case clientCertSubjectOrganization:
				if err := testCertValuesSlicesStringSubject(clientCertSubjectOrganization, v, cert.Subject.Organization); err != nil {
					return err
				}
			case clientCertSubjectOrganizationalUnit:
				if err := testCertValuesSlicesStringSubject(clientCertSubjectOrganizationalUnit, v, cert.Subject.OrganizationalUnit); err != nil {
					return err
				}
			}
		}
	case ClientCertificateSubjectPrefixPattern:
		for k, v := range ccs.regexpkvs {
			switch k {
			case clientCertSubjectCommonName:
				expected := []string{cert.Subject.CommonName}
				if err := testCertValuesSlicesRegexpSubject(clientCertSubjectCommonName, v, expected); err != nil {
					return err
				}
			case clientCertSubjectCountry:
				if err := testCertValuesSlicesRegexpSubject(clientCertSubjectCountry, v, cert.Subject.Country); err != nil {
					return err
				}
			case clientCertSubjectProvince:
				if err := testCertValuesSlicesRegexpSubject(clientCertSubjectProvince, v, cert.Subject.Province); err != nil {
					return err
				}
			case clientCertSubjectLocality:
				if err := testCertValuesSlicesRegexpSubject(clientCertSubjectLocality, v, cert.Subject.Locality); err != nil {
					return err
				}
			case clientCertSubjectOrganization:
				if err := testCertValuesSlicesRegexpSubject(clientCertSubjectOrganization, v, cert.Subject.Organization); err != nil {
					return err
				}
			case clientCertSubjectOrganizationalUnit:
				if err := testCertValuesSlicesRegexpSubject(clientCertSubjectOrganizationalUnit, v, cert.Subject.OrganizationalUnit); err != nil {
					return err
				}
			}
		}
	default:
		return errors.New("unknown subject header")
	}

	return nil
}

func testCertValuesSlicesStringSubject(field string, expected, certValues []string) error {
	sort.Strings(expected)
	sort.Strings(certValues)
	if !compareStringSlices(expected, certValues) {
		return ClientCertificateRejectedError{
			Field:    clientCertSubjectOrganizationalUnit,
			Expected: expected,
			Received: certValues,
		}
	}
	return nil
}

func testCertValuesSlicesRegexpSubject(field string, expected []*regexp.Regexp, certValues []string) error {
	if len(expected) != len(certValues) {
		return fmt.Errorf("%s: expected has %d elements vs %d in certificate", field, len(expected), len(certValues))
	}
	for index, certValue := range certValues {
		if matched := expected[index].Match([]byte(certValue)); !matched {
			return fmt.Errorf("%s: rejected, '%s' did not match '%s'", field, expected[index], certValue)
		}
	}
	return nil
}

func compareStringSlices(this, that []string) bool {
	if len(this) != len(that) {
		return false
	}
	for index := range this {
		if this[index] != that[index] {
			return false
		}
	}
	return true
}
