package googleid

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestGetUserIDToken(t *testing.T) {
	t.Skip() // Uncomment to execute

	src := NewAuthorizedUserTokenSource()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	token, err := src.GetIDToken(ctx)

	a := assert.New(t)
	a.Nil(err)
	a.NotEmpty(token)
}
