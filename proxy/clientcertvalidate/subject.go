package clientcertvalidate

import (
	"crypto/x509"
	"errors"
	"fmt"
	"regexp"
	"sort"
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
	Input() string
	Type() ClientCertificateSubjectPrefixType
	X509Validate(*x509.Certificate) error
}

type defaultParsedSubject struct {
	input           string
	inputValuesType ClientCertificateSubjectPrefixType
	kvs             KVs
}

func (ccs *defaultParsedSubject) KVs() KVs {
	return ccs.kvs
}

func (ccs *defaultParsedSubject) Input() string {
	return ccs.input
}

func (ccs *defaultParsedSubject) Type() ClientCertificateSubjectPrefixType {
	return ccs.inputValuesType
}

func (ccs *defaultParsedSubject) X509Validate(cert *x509.Certificate) error {
	for k, v := range ccs.kvs {
		switch k {
		case clientCertSubjectCommonName:
			expected := []string{cert.Subject.CommonName}
			if err := testCertValuesSlices(clientCertSubjectCommonName, v,
				expected, ccs.inputValuesType); err != nil {
				return err
			}
		case clientCertSubjectCountry:
			if err := testCertValuesSlices(clientCertSubjectCountry, v,
				cert.Subject.Country, ccs.inputValuesType); err != nil {
				return err
			}
		case clientCertSubjectProvince:
			if err := testCertValuesSlices(clientCertSubjectProvince, v,
				cert.Subject.Province, ccs.inputValuesType); err != nil {
				return err
			}
		case clientCertSubjectLocality:
			if err := testCertValuesSlices(clientCertSubjectLocality, v,
				cert.Subject.Locality, ccs.inputValuesType); err != nil {
				return err
			}
		case clientCertSubjectOrganization:
			if err := testCertValuesSlices(clientCertSubjectOrganization, v,
				cert.Subject.Organization, ccs.inputValuesType); err != nil {
				return err
			}
		case clientCertSubjectOrganizationalUnit:
			if err := testCertValuesSlices(clientCertSubjectOrganizationalUnit, v,
				cert.Subject.OrganizationalUnit, ccs.inputValuesType); err != nil {
				return err
			}
		}
	}

	return nil
}

func testCertValuesSlices(field string, expected, certValues []string, headerType ClientCertificateSubjectPrefixType) error {
	if headerType == ClientCertificateSubjectPrefixString { // easy: just sort and check if true:
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
	if headerType == ClientCertificateSubjectPrefixPattern {
		if len(expected) != len(certValues) {
			return fmt.Errorf("%s: expected has %d elements vs %d in certificate", field, len(expected), len(certValues))
		}
		for index, certValue := range certValues {
			matched, err := regexp.Match(expected[index], []byte(certValue))
			if err != nil {
				return fmt.Errorf("%s: rejected, '%s', not valid regexp? reason: %v", field, expected[index], err)
			}
			if !matched {
				return fmt.Errorf("%s: rejected, '%s' did not match '%s'", field, expected[index], certValue)
			}
		}
		return nil
	}
	return errors.New("unknown subject header")
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
