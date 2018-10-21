package util

import "fmt"

type ArrayFlags []string

func (i *ArrayFlags) String() string {
	return fmt.Sprintf("%v", *i)
}

func (i *ArrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func (i *ArrayFlags) AsMap() map[string]struct{} {
	result := make(map[string]struct{})
	for _, elem := range *i {
		result[elem] = struct{}{}
	}
	return result
}
