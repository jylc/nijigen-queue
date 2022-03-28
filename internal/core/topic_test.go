package core

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetTopic(t *testing.T) {
	nq := NewNQ()
	assert.NotNil(t, nq)
	topicName := "Topic_test_1"
	GetTopic(topicName, nq)
	topic := nq.topicMap[topicName]
	assert.NotNil(t, topic)
}

func TestTopic_GetChannel(t *testing.T) {
	nq := NewNQ()
	assert.NotNil(t, nq)
	topicName := "Topic_test_1"
	GetTopic(topicName, nq)
	topic := nq.topicMap[topicName]
	assert.NotNil(t, topic)
	channelName1 := "Channel_test_1"
	channelName2 := "Channel_test_2"
	topic.GetChannel(channelName1)
	topic.GetChannel(channelName2)
	channel1 := topic.chmap[channelName1]
	channel2 := topic.chmap[channelName2]
	assert.NotNil(t, channel1)
	assert.NotNil(t, channel2)
}
