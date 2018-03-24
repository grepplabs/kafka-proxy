package registry

import (
	"fmt"
	"github.com/grepplabs/kafka-proxy/pkg/apis"
	"github.com/stretchr/testify/assert"
	"testing"
)

type tokenProviderFactory struct {
}

func (t *tokenProviderFactory) New(params []string) (apis.TokenProvider, error) {
	fmt.Println("TokenProviderFactory is executed")
	return nil, nil
}

type tokenInfoFactory struct {
}

func (t *tokenInfoFactory) New(params []string) (apis.TokenInfo, error) {
	fmt.Println("TokenProviderFactory is executed")
	return nil, nil
}

func TestRegistry(t *testing.T) {
	a := assert.New(t)
	a.Nil(nil)

	NewComponentInterface(new(apis.TokenProviderFactory))
	NewComponentInterface(new(apis.TokenInfoFactory))

	Register(new(tokenProviderFactory), "googleid-provider")

	component1, ok := GetComponent(new(apis.TokenProviderFactory), "googleid-provider").(apis.TokenProviderFactory)
	a.True(ok)
	a.NotNil(component1)

	component2, ok := GetComponent(new(apis.TokenInfoFactory), "googleid-info").(apis.TokenInfoFactory)
	a.False(ok)
	a.Nil(component2)

	NewComponentInterface(new(apis.TokenProviderFactory))
	NewComponentInterface(new(apis.TokenInfoFactory))

	Register(new(tokenInfoFactory), "googleid-info")

	component1, ok = GetComponent(new(apis.TokenProviderFactory), "googleid-provider").(apis.TokenProviderFactory)
	a.True(ok)
	a.NotNil(component1)

	component2, ok = GetComponent(new(apis.TokenInfoFactory), "googleid-info").(apis.TokenInfoFactory)
	a.True(ok)
	a.NotNil(component2)
}
