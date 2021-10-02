package queue

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
)

const (
	Interval = 100 * time.Millisecond
	SlotNum  = 4
	Capacity = 100
)

func TestNewNQTimeWheel(t *testing.T) {
	sendChan := make(chan interface{})
	_, err := NewNQTimeWheel(0, SlotNum, Capacity, sendChan)
	fmt.Printf("%s", Interval)
	assert.NotNil(t, err)
}

func TestTimeWheel_Set(t *testing.T) {
	sendChan := make(chan interface{})
	wg := &sync.WaitGroup{}
	tw, err := NewNQTimeWheel(Interval, SlotNum, Capacity, sendChan)
	assert.Nil(t, err)
	assert.NotNil(t, tw)
	go func() {
		wg.Add(1)
		for i := 0; i < 1000; i++ {
			select {
			case value := <-sendChan:
				if item, ok := value.(*Item); ok {
					fmt.Println("accept:" + item.Value.(string))
				} else {
					fmt.Println("type error")
				}
			}
		}
		wg.Done()
	}()
	for i := 0; i < 1000; i++ {
		latency := time.Duration(rand.Int63()%1000) * time.Millisecond
		value := fmt.Sprintf("message-%d:latency = %s", i, latency)
		fmt.Println(value)
		tw.Set(latency, value)
	}
	wg.Wait()
	tw.Stop()
}
