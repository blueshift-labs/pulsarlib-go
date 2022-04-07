package pulsarlib

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessagingInit(t *testing.T) {
	err := InitMessaging(10, "localhost", 6650, 8080)
	assert.Nil(t, err)
}
