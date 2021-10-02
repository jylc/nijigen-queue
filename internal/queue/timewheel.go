package queue

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type TimeWheel struct {
	interval  time.Duration
	ticker    *time.Ticker
	tickerPos int
	slots     []*list.List
	slotNum   int
	itemChan  chan interface{}
	sendChan  chan interface{}
	stopChan  chan interface{}
	lock      sync.RWMutex
}

func NewNQTimeWheel(interval time.Duration, slotNum int, capacity int, sendChan chan interface{}) (*TimeWheel, error) {
	if interval <= 0 || slotNum <= 0 || capacity <= 0 {
		logrus.Errorf("init timewheel failed")
		return nil, fmt.Errorf("interval: %v, slots: %d, capacity: %d", interval, slotNum, capacity)
	}
	t := &TimeWheel{
		interval:  interval,
		ticker:    time.NewTicker(interval),
		slots:     make([]*list.List, slotNum),
		slotNum:   slotNum,
		tickerPos: 0,
		itemChan:  make(chan interface{}),
		stopChan:  make(chan interface{}),
		sendChan:  sendChan,
	}
	t.initSlots()
	go t.run()
	return t, nil
}

func (t *TimeWheel) run() {
	for {
		select {
		case <-t.ticker.C:
			t.tickerPos = (t.tickerPos + 1) % t.slotNum
			l := t.slots[t.tickerPos]
			t.scanAndFlush(l)
		case item := <-t.itemChan:
			t.put(item)
		case <-t.stopChan:
			t.ticker.Stop()
			return
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

func (t *TimeWheel) put(value interface{}) {
	t.lock.Lock()
	defer t.lock.Unlock()
	item := value.(*Item)
	latency := item.Latency
	level := 0
	if latency > t.interval*time.Duration(t.slotNum) {
		level = int(latency / (t.interval * time.Duration(t.slotNum)))
		latency = time.Duration(int(latency % (t.interval * time.Duration(t.slotNum))))
	}
	step := int(latency / t.interval)
	pos := (t.tickerPos + step) % t.slotNum
	slot := t.slots[pos]
	item.Latency = latency % t.interval
	item.Level = level
	if item.Latency < 0 {
		logrus.Errorf("left time need larger than 0")
		return
	}
	logrus.Infof("ticker pos: %d,stored pos: %d,level: %d,latency: %s",
		t.tickerPos, pos, level, latency)
	slot.PushBack(value)
}

func (t *TimeWheel) scanAndFlush(l *list.List) {
	for e := l.Front(); e != nil; {
		item := e.Value.(*Item)
		if item.Level == 0 {
			t.sendChan <- item
			next := e.Next()
			l.Remove(e)
			e = next
			continue
		} else if item.Level != 0 {
			item.Level--
			e = e.Next()
			continue
		}
	}
}

func (t *TimeWheel) initSlots() {
	for i := 0; i < t.slotNum; i++ {
		t.slots[i] = list.New()
	}
}

func (t *TimeWheel) Stop() {
	close(t.stopChan)
}
