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
	p.pos = p.pos + 1
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

	output := &defaultParsedSubject{
		kvs: make(KVs),
	}

	prefix, err := p.consume(2) // read prefix
	if err != nil {
		// did not consume enough:
		return output, &ParserMissingPrefixError{RawInput: string(p.input)}
	}
	if (prefix[0] == 's' || prefix[0] == 'r') && prefix[1] == ':' {
		switch prefix[0] {
		case 's':
			output.inputValuesType = ClientCertificateSubjectPrefixString
		case 'r':
			output.inputValuesType = ClientCertificateSubjectPrefixPattern
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
					Expected: '/',
					Found:    nextRune,
					Position: p.pos,
				}
			}
			p.skip(1) // skip /
			// read the key:
			fieldPos := p.pos
			field, err := p.readAlphaStringUntil('=')
			if err == io.EOF {
				// there was no key:
				return output, &ParserUnexpectedInputError{
					Expected: "field",
					Found:    "none",
					Position: fieldPos,
				}
			}
			if err == os.ErrInvalid {
				// there was no key:
				return output, &ParserInvalidSubjectFieldError{ConsumedString: field}
			}

			if _, ok := validSubjectFields[field]; !ok {
				return output, &ParserUnsupportedSubjectFieldError{Field: field}
			}

			// key has been consumed, value must be enclosed in [...]
			nextRune, lookupErr = p.lookupOne()
			if lookupErr == io.EOF {
				return output, &ParserUnexpectedInputError{
					Expected: '[',
					Found:    "none",
					Position: p.pos,
				}
			}
			if nextRune != '[' {
				return output, &ParserUnexpectedInputError{
					Expected: '[',
					Found:    nextRune,
					Position: p.pos,
				}
			}

			// read value, everything until closing ]:
			valuePos := p.pos
			values, err := p.readValues(output.inputValuesType)
			switch err {
			case io.EOF:
				return output, fmt.Errorf("expected value enclosed within [] at position %d but received '%v' and run out of input", valuePos, values)
			case errUnexpectedPattern:
				return output, fmt.Errorf("value starting at %d contains patterns but subject is type string", valuePos)
			}

			if output.inputValuesType == ClientCertificateSubjectPrefixPattern {
				for _, pattern := range values {
					if _, compileErr := regexp.Compile(pattern); compileErr != nil {
						return output, &InvalidPatternValueError{
							Field:  field,
							Reason: compileErr,
							Value:  pattern,
						}
					}
				}
			}

			output.kvs[field] = values
		}

		return output, nil
	}

	return output, &ParserInvalidPrefixError{ConsumedPrefix: string(prefix)}
}

func (p *defaultSubjectParser) readAlphaStringUntil(boundary rune) (string, error) {
	consumed := []rune{}
	for {
		nextRune, err := p.lookupOne()

		if err == io.EOF {
			return string(consumed), io.EOF
		}

		if unicode.IsLetter(nextRune) && unicode.IsUpper(nextRune) {
			// just use the lookup value and skip:
			consumed = append(consumed, nextRune)
			p.skip(1)
			continue
		}
		if nextRune == '=' {
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

func (p *defaultSubjectParser) readValues(inputValuesType ClientCertificateSubjectPrefixType) ([]string, error) {
	values := []string{}
	consumed := []rune{}
	insidePattern := false
	nest := 0
	for {

		nextRune, err := p.lookupOne()

		if err == io.EOF {
			return values, io.EOF
		}

		if nextRune == '[' {
			nest = nest + 1
			if nest > 1 {
				if inputValuesType == ClientCertificateSubjectPrefixString {
					// a string type subject can't contain nested patterns:
					return values, errUnexpectedPattern
				}
			} else {
				p.skip(1)
				continue
			}
		}

		if nextRune == ']' {
			if nest-1 == 0 {
				// we have reached the end of the value:
				p.skip(1)
				break
			}
			if nest == 1 {
				p.skip(1)
				continue
			}
			nest = nest - 1
		}

		// values are separated by commas but they can only exist when we are inside
		// top level [...] and not between {...}
		if nextRune == '{' {
			// nested {} are not okay...
			if insidePattern {
				return values, errNestedPattern
			}
			insidePattern = true
		}

		if nextRune == '}' {
			insidePattern = false
		}

		if nextRune == ',' {
			if !insidePattern && nest == 1 { // value separator
				values = append(values, string(consumed))
				consumed = []rune{}
				p.skip(1)
				continue
			}
		}

		// everything else is a part of the value:
		consumed = append(consumed, nextRune)
		p.skip(1)

	}

	if len(consumed) > 0 {
		values = append(values, string(consumed))
	}

	return values, nil
}
