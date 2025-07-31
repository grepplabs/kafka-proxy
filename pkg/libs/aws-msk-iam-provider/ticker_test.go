package awsmskiamprovider

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTokenRefresher_creation(t *testing.T) {
	provider := &TokenProvider{
		timeout: 10 * time.Second,
		region:  "us-east-1",
	}

	refresher := &TokenRefresher{
		tokenProvider: provider,
		stopChannel:   make(chan bool, 1),
	}

	assert.NotNil(t, refresher)
	assert.Equal(t, provider, refresher.tokenProvider)
	assert.NotNil(t, refresher.stopChannel)
}

func TestTokenRefresher_stopChannel(t *testing.T) {
	provider := &TokenProvider{
		timeout: 10 * time.Second,
		region:  "us-east-1",
	}

	refresher := &TokenRefresher{
		tokenProvider: provider,
		stopChannel:   make(chan bool, 1),
	}

	// Test that stop channel can receive a signal
	refresher.stopChannel <- true
	select {
	case <-refresher.stopChannel:
		// Successfully received the stop signal
	default:
		t.Error("Stop channel should be able to receive signals")
	}
}
