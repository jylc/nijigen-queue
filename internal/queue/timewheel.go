package queue

import (
	"math"
	"time"

	"github.com/sirupsen/logrus"
)

type TimeWheel struct {
	interval   time.Duration
	ticker     *time.Ticker
	tickerPos  int
	slots      []*PriorityQueue
	slotNum    int
	itemChan   chan interface{}
	notifyChan chan interface{}
	sendChan   chan interface{}
}

func NewNQTimeWheel(interval time.Duration, slotNum int, capacity int, sendChan chan interface{}) *TimeWheel {
	if interval < 0 || slotNum < 0 || capacity < 0 {
		logrus.Errorf("init timewheel failed")
		return nil
	}
	t := &TimeWheel{
		interval:   interval,
		ticker:     time.NewTicker(interval),
		slots:      NewNQPriorityQueues(slotNum, capacity),
		slotNum:    slotNum,
		tickerPos:  0,
		itemChan:   make(chan interface{}),
		notifyChan: make(chan interface{}),
		sendChan:   sendChan,
	}
	go t.run()
	return t
}

func (t *TimeWheel) run() {
	for {
		select {
		case <-t.ticker.C:
			t.tickerPos = (t.tickerPos + 1) % t.slotNum
			slot := t.slots[t.tickerPos]
			item := slot.Pop()
			t.sendChan <- item
		case item := <-t.itemChan:
			t.put(item)
		}
	}
}

func (t *TimeWheel) Set(latency time.Duration, value interface{}) {
	item := &Item{
		Latency: latency,
		Value:   value,
	}
	t.itemChan <- item
}

func (t *TimeWheel) put(item interface{}) {
	latency := item.(*Item).Latency
	if latency < t.interval*time.Duration(t.slotNum) {
		//延时时间小于时间轮的最大时间
		n := int(math.Floor(float64(latency / t.interval)))
		pos := t.tickerPos + n
		slot := t.slots[pos]
		slot.Push(item)
	} else {
		//延时时间小于时间轮的最大时间
	}
}
