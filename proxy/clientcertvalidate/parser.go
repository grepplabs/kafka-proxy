package clientcertvalidate

import (
	"fmt"
	"io"
	"os"
	"unicode"

	"github.com/pkg/errors"
)

var errUnexpectedPattern = errors.New("errUnexpectedPattern")
var errNestedPattern = errors.New("errNestedPattern")

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

	header, err := p.consume(2) // read header
	if err != nil {
		// did not consume enough:
		return output, errors.New("no header found")
	}
	if (header[0] == 's' || header[0] == 'r') && header[1] == ':' {
		switch header[0] {
		case 's':
			output.inputValuesType = ClientCertificateHeaderString
		case 'r':
			output.inputValuesType = ClientCertificateHeaderPattern
		}
		// header is valid, read KVs:

		for { // until EOF
			nextRune, lookupErr := p.lookupOne()
			if lookupErr == io.EOF {
				break // done, all parsed
			}
			if nextRune != '/' {
				// error, expected / at position
				return output, fmt.Errorf("expected / at position %d but received %v", p.pos, nextRune)
			}
			p.skip(1) // skip /
			// read the key:
			keyPos := p.pos
			key, err := p.readAlphaStringUntil('=')
			if err == io.EOF {
				// there was no key:
				return output, fmt.Errorf("expected key at position %d but none received", keyPos)
			}
			if err == os.ErrInvalid {
				// there was no key:
				return output, fmt.Errorf("consumed '%s' is no a valid key, valid key contains uppercase letters only", key)
			}

			// key has been consumed, value must be enclosed in [...]
			nextRune, lookupErr = p.lookupOne()
			if lookupErr == io.EOF {
				return output, fmt.Errorf("expected [ at position %d but nothing received", p.pos)
			}
			if nextRune != '[' {
				return output, fmt.Errorf("expected [ at position %d but received %v", p.pos, nextRune)
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

			output.kvs[key] = values
		}

		return output, nil
	}

	return output, errors.New("subject header invalid, s: or r: expected")
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

func (p *defaultSubjectParser) readValues(inputValuesType ClientCertificateHeaderType) ([]string, error) {
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
				if inputValuesType == ClientCertificateHeaderString {
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
