package clientcertvalidate

import (
	"fmt"

	"github.com/pkg/errors"
)

var errUnexpectedPattern = errors.New("errUnexpectedPattern")
var errNestedPattern = errors.New("errNestedPattern")

// ClientCertificateRejectedError contains the details of the certificate rejection reason.
type ClientCertificateRejectedError struct {
	// Subject field that did not match.
	Field string
	// Expected values given in the subject argument for the given field.
	Expected interface{}
	// Values found in the certificate subject field for the given field.
	Received interface{}
}

// Error returns the string representation of the error.
func (e ClientCertificateRejectedError) Error() string {
	return fmt.Sprintf("cert rejected: client certificate rejected: subject field '%s' expected '%v' but received '%v'",
		e.Field, e.Expected, e.Received)
}

// InvalidPatternValueError contains the details of the certificate rejection reason.
type InvalidPatternValueError struct {
	// Field in which the value is defined:
	Field string
	// Original error
	Reason error
	// Value which is problematic.
	Value string
}

// Error returns the string representation of the error.
func (e InvalidPatternValueError) Error() string {
	return fmt.Sprintf("invalid pattern: field '%s' contains value '%s' which did not compile as regex, reason: '%v'",
		e.Field, e.Value, e.Reason)
}

// ParserValueInsufficientInputError is an error returned when a value list can't be fully parsed because there was no more input.
type ParserValueInsufficientInputError struct {
	// Consumed string at the expected field position.
	ValuePos int
}

// Error returns the string representation of the error.
func (e ParserValueInsufficientInputError) Error() string {
	return fmt.Sprintf("parser: value insufficient input: value at position '%d' run out of input, expected closing", e.ValuePos)
}

// ParserInvalidSubjectFieldError is an error returned when a string consumed at a field position
// unsupported is not a valid field string.
type ParserInvalidSubjectFieldError struct {
	// Consumed string at the expected field position.
	ConsumedString string
}

// Error returns the string representation of the error.
func (e ParserInvalidSubjectFieldError) Error() string {
	return fmt.Sprintf("parser: invalid field: value '%s' is not a valid field", e.ConsumedString)
}

// ParserInvalidPrefixError is returned when an unsupported prefix is consumed.
type ParserInvalidPrefixError struct {
	// Consumed invalid value
	ConsumedPrefix string
}

// Error returns the string representation of the error.
func (e ParserInvalidPrefixError) Error() string {
	return fmt.Sprintf("parser: invalid prefix: consumed '%q' is not a valid prefix, s: and r: are supported", e.ConsumedPrefix)
}

// ParserUnexpectedInputError is returned when an unexpected token is found in the parsed input.
type ParserUnexpectedInputError struct {
	// What was expected.
	Expected []rune
	// Unexpected input.
	Found []rune
	// Position of the input.
	Position int
}

// Error returns the string representation of the error.
func (e ParserUnexpectedInputError) Error() string {
	return fmt.Sprintf("parser: unexpected input: expected '%v' at position '%d' but found '%s'", string(e.Expected), e.Position, string(e.Found))
}

// ParserMissingPrefixError is returned when an input string is missing a prefix.
type ParserMissingPrefixError struct {
	// Raw input.
	RawInput string
}

// Error returns the string representation of the error.
func (e ParserMissingPrefixError) Error() string {
	return fmt.Sprintf("parser: no prefix: input '%q' is missing a prefix", e.RawInput)
}

// ParserUnsupportedSubjectFieldError is an error returned when an unsupported
// subject field is consumed by the parser.
type ParserUnsupportedSubjectFieldError struct {
	// Consumed unsupported field that did not match.
	Field string
}

// Error returns the string representation of the error.
func (e ParserUnsupportedSubjectFieldError) Error() string {
	return fmt.Sprintf("parser: unsupported field: field %s is not supported", e.Field)
}

// ParserUnexpectedError is an error returned when an unexpected error is encountered.
type ParserUnexpectedError struct {
	// The unexpected raw cause.
	Unexpected error
}

// Error returns the string representation of the error.
func (e ParserUnexpectedError) Error() string {
	return fmt.Sprintf("parser: unexpected error: %v", e.Unexpected)
}
