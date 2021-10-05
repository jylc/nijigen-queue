package core

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/jylc/nijigen-queue/internal/message"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestNewChannel(t *testing.T) {
	nq := NewNQ()
	assert.NotNil(t, nq)
	ch := NewChannel("channel-1", nq)
	assert.NotNil(t, ch)
}

func TestChannel_Publish(t *testing.T) {
	nq := NewNQ()
	assert.NotNil(t, nq)
	ch := NewChannel("channel-1", nq)
	go func() {
		for i := 0; i < 1000; i++ {
			msg := &message.MetaMessage{
				Topic:    "topic-1",
				Channel:  "channel-1",
				Content:  "message-" + strconv.Itoa(i),
				Latency:  time.Duration(rand.Int63()%10+6) * time.Second,
				Timeout:  0,
				Deadline: time.Timer{},
			}
			if err := ch.Publish(msg); err != nil {
				logrus.Error(err)
			}
		}
	}()
	for i := 0; i < 1000; i++ {
		select {
		case value := <-ch.testChan:
			msg := value.(*message.MetaMessage)
			logrus.Infof("content:%s,latency:%s", msg.Content, msg.Latency)
		}
	}
}
