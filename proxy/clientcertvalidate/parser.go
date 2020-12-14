package clientcertvalidate

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"unicode"
)

// KVs represents parsed key value client certificate subject.
type KVs map[string][]string

// RegexpKVs represents compiled regexes for parsed key value client certificate subject
// when the subject type is a pattern.
type RegexpKVs map[string][]*regexp.Regexp

// SubjectParser represents the subject parser.
type SubjectParser interface {
	Parse() (ParsedSubject, error)
}

// NewSubjectParser creates a new default subject parser.
func NewSubjectParser(input string) SubjectParser {
	runeInput := []rune(input)
	return &defaultSubjectParser{
		input:  runeInput,
		pos:    0,
		maxPos: len(input),
	}
}

type defaultSubjectParser struct {
	input  []rune
	pos    int
	maxPos int
}

func (p *defaultSubjectParser) consumeOne() (rune, error) {
	runes, err := p.consume(1)
	if err != nil {
		return ' ', io.EOF
	}
	return runes[0], nil
}

func (p *defaultSubjectParser) consume(n int) ([]rune, error) {
	var err error
	if p.pos+n > p.maxPos {
		err = io.EOF
		n = p.maxPos - p.pos
	}
	if n == 0 {
		return []rune{}, io.EOF
	}
	defer func() { p.pos = p.pos + n }()
	return []rune(p.input[p.pos : p.pos+n]), err
}

func (p *defaultSubjectParser) skip(n int) {
	p.pos = p.pos + n
}

func (p *defaultSubjectParser) lookupOne() (rune, error) {
	runes, err := p.lookup(1)
	if err != nil {
		return ' ', io.EOF
	}
	return runes[0], nil
}

func (p *defaultSubjectParser) lookup(n int) ([]rune, error) {
	var err error
	if p.pos+n > p.maxPos {
		err = io.EOF
		n = p.maxPos - p.pos
	}
	if n == 0 {
		return []rune{}, io.EOF
	}
	return []rune(p.input[p.pos : p.pos+n]), err
}

// Parse parses the parser input as a subject.
func (p *defaultSubjectParser) Parse() (ParsedSubject, error) {

	output := DefaultParsedSubject()

	prefix, err := p.consume(2) // read prefix
	if err != nil {
		// did not consume enough:
		return output, &ParserMissingPrefixError{RawInput: string(p.input)}
	}
	if (prefix[0] == 's' || prefix[0] == 'r') && prefix[1] == ':' {
		switch prefix[0] {
		case 's':
			output.WithType(ClientCertificateSubjectPrefixString)
		case 'r':
			output.WithType(ClientCertificateSubjectPrefixPattern)
		}
		// prefix is valid, read KVs:

		for { // until EOF
			nextRune, lookupErr := p.lookupOne()
			if lookupErr == io.EOF {
				break // done, all parsed
			}
			if nextRune != '/' {
				// error, expected / at position
				return output, &ParserUnexpectedInputError{
					Expected: []rune{'/'},
					Found:    []rune{nextRune},
					Position: p.pos,
				}
			}
			p.skip(1) // skip /
			// read the key:
			fieldPos := p.pos
			field, err := p.readAlphaStringUntil('=')
			if err != nil {
				switch err {
				case io.EOF:
					// there was no key:
					return output, &ParserUnexpectedInputError{
						Expected: []rune("field"),
						Found:    []rune("none"),
						Position: fieldPos,
					}
				case os.ErrInvalid:
					// there was no key:
					return output, &ParserInvalidSubjectFieldError{ConsumedString: field}
				default:
					return output, &ParserUnexpectedError{Unexpected: err}
				}
			}

			if _, ok := validSubjectFields[field]; !ok {
				return output, &ParserUnsupportedSubjectFieldError{Field: field}
			}

			// next rune must be value list start: [

			// key has been consumed, value must be enclosed in [...]
			nextRune, lookupErr = p.lookupOne()
			if lookupErr != nil {
				switch lookupErr {
				case io.EOF:
					return output, &ParserUnexpectedInputError{
						Expected: []rune{'['},
						Found:    []rune("none"),
						Position: p.pos,
					}
				default:
					return output, &ParserUnexpectedError{Unexpected: err}
				}
			}
			if nextRune != '[' {
				return output, &ParserUnexpectedInputError{
					Expected: []rune{'['},
					Found:    []rune{nextRune},
					Position: p.pos,
				}
			}

			// read value, everything until closing ]:
			valuePos := p.pos
			values, err := p.readValues()
			if err != nil {
				switch err {
				case io.EOF:
					return output, &ParserValueInsufficientInputError{ValuePos: valuePos}
				case errUnexpectedPattern:
					return output, fmt.Errorf("value starting at %d contains patterns but subject is type string", valuePos)
				default:
					return output, &ParserUnexpectedError{Unexpected: err}
				}
			}

			regexpKVs := []*regexp.Regexp{}
			if output.Type() == ClientCertificateSubjectPrefixPattern {
				for _, pattern := range values {
					compiledRegexp, compileErr := regexp.Compile(pattern)
					if compileErr != nil {
						return output, &InvalidPatternValueError{
							Field:  field,
							Reason: compileErr,
							Value:  pattern,
						}
					}
					regexpKVs = append(regexpKVs, compiledRegexp)
				}
			}

			output.WithKVs(field, values)
			output.WithRegexpKVs(field, regexpKVs)
		}

		return output, nil
	}

	return output, &ParserInvalidPrefixError{ConsumedPrefix: string(prefix)}
}

func (p *defaultSubjectParser) readAlphaStringUntil(boundary rune) (string, error) {
	consumed := []rune{}
	for {
		nextRune, err := p.lookupOne()

		if err != nil {
			return string(consumed), err
		}

		if unicode.IsLetter(nextRune) && unicode.IsUpper(nextRune) {
			// just use the lookup value and skip:
			consumed = append(consumed, nextRune)
			p.skip(1)
			continue
		}
		if nextRune == boundary {
			// all good, break
			p.skip(1)
			break
		}
		consumed = append(consumed, nextRune)
		return string(consumed), os.ErrInvalid
	}
	if len(consumed) == 0 {
		return "", io.EOF
	}
	return string(consumed), nil
}

func (p *defaultSubjectParser) readAnyEnclosed(boundaryStart, boundaryEnd rune, inclusive bool) (string, error) {
	consumed := []rune{}
	for {
		nextRune, err := p.lookupOne()
		if err != nil {
			return string(consumed), err
		}
		if nextRune == boundaryEnd {
			if inclusive {
				p.skip(1)
				consumed = append(consumed, nextRune)
			}
			break
		}
		if nextRune == boundaryStart {
			p.skip(1)
			consumed = append(consumed, nextRune)
			chunk, err := p.readAnyEnclosed(boundaryStart, boundaryEnd, inclusive)
			if err != nil {
				return string(consumed), err
			}
			consumed = append(consumed, []rune(chunk)...)
			continue
		}
		if nextRune == '\\' {
			chunk, err := p.readEscapeSequence()
			if err != nil {
				return string(consumed), err
			}
			consumed = append(consumed, []rune(chunk)...)
			continue
		}
		p.skip(1)
		consumed = append(consumed, nextRune)
	}
	if len(consumed) == 0 {
		return "", io.EOF
	}
	return string(consumed), nil
}

// Reads top level key value values. These have to be enclosed in [...].
// Tries reading [, followed by any literal value separated by comma and followed by closing ].
func (p *defaultSubjectParser) readValues() ([]string, error) {

	values := []string{}

	nextRune, err := p.consumeOne()
	if err != nil {
		return values, err
	}
	if nextRune != '[' {
		return values, &ParserUnexpectedInputError{
			Expected: []rune{'['},
			Found:    []rune{nextRune},
			Position: p.pos,
		}
	}

	nextValue, readValuesErr := p.readValue()
	if readValuesErr != nil {
		return values, readValuesErr
	}
	values = append(values, nextValue)
	for {
		nextRune, err := p.lookupOne()
		if err != nil {
			return values, err
		}
		if nextRune == ',' {
			p.skip(1)
			nextValue, readValuesErr := p.readValue()
			if readValuesErr != nil {
				return values, readValuesErr
			}
			values = append(values, nextValue)
			continue
		}
		break
	}

	nextRune, err = p.consumeOne()
	if err != nil {
		return values, err
	}
	if nextRune != ']' {
		return values, &ParserUnexpectedInputError{
			Expected: []rune{']'},
			Found:    []rune{nextRune},
			Position: p.pos,
		}
	}

	return values, nil
}

// Read a single value. A valid value is any literal until unescaped , or ].
func (p *defaultSubjectParser) readValue() (string, error) {
	currentValue := []rune{}
	for {

		nextRune, err := p.lookupOne()
		if err != nil {
			return string(currentValue), err
		}

		if nextRune == '[' {
			boundaryEnd := ']'
			// consume this bracket:
			p.skip(1)
			currentValue = append(currentValue, nextRune)
			// followed by everything until matching closing bracket:
			value, err := p.readAnyEnclosed(nextRune, boundaryEnd, true)
			if err != nil {
				return string(currentValue), err
			}
			currentValue = append(currentValue, []rune(value)...)
			continue
		}

		if nextRune == '{' {
			boundaryEnd := '}'
			// consume this bracket:
			p.skip(1)
			currentValue = append(currentValue, nextRune)
			// followed by everything until matching closing bracket:
			value, err := p.readAnyEnclosed(nextRune, boundaryEnd, true)
			if err != nil {
				return string(currentValue), err
			}
			currentValue = append(currentValue, []rune(value)...)
			continue
		}

		if nextRune == ',' {
			// do not consume the comma, caller checks for its existence
			break
		}

		// if we find an unmatched value limiter
		// unless escaped, break out:
		if nextRune == ']' {
			// we're done, done skip, caller checks for closing bracket:
			break
		}

		if nextRune == '\\' {
			chunk, err := p.readEscapeSequence()
			if err != nil {
				return string(currentValue), err
			}
			currentValue = append(currentValue, []rune(chunk)...)
			continue
		}

		// otherwise, just consume the character:
		p.skip(1)
		currentValue = append(currentValue, nextRune)
	}

	return string(currentValue), nil
}

func (p *defaultSubjectParser) readEscapeSequence() (string, error) {
	currentValue := []rune{}
	nextRune, err := p.consumeOne()
	if err != nil {
		return string(currentValue), err
	}
	if nextRune != '\\' {
		return "", &ParserUnexpectedInputError{
			Expected: []rune{'\\'},
			Found:    []rune{nextRune},
			Position: p.pos - 1,
		}
	}
	currentValue = append(currentValue, nextRune)

	nextRune, err = p.lookupOne()
	if err != nil {
		return string(currentValue), err
	}

	// otherwise, whatever that is, just consume it
	p.skip(1)
	currentValue = append(currentValue, nextRune)
	return string(currentValue), nil
}
