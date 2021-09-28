package queue

import (
	"time"
)

type Item struct {
	Latency time.Duration
	Value   interface{}
}

//PriorityQueue save the deferred message which do not send to Consumer at once.
type PriorityQueue struct {
	items []*Item
}

func NewNQPriorityQueue(capacity int) *PriorityQueue {
	return &PriorityQueue{items: make([]*Item, capacity)}
}

func NewNQPriorityQueues(size int, capacity int) (queues []*PriorityQueue) {
	for i := 0; i < size; i++ {
		queues = append(queues, NewNQPriorityQueue(capacity))
	}
	return
}

func (q *PriorityQueue) Len() int { return len(q.items) }

func (q *PriorityQueue) Less(i, j int) bool {
	return q.items[i].Latency < q.items[j].Latency
}

func (q *PriorityQueue) Swap(i, j int) {
	q.items[i], q.items[j] = q.items[j], q.items[i]
}

func (q *PriorityQueue) Push(item interface{}) {
	q.items = append(q.items, item.(*Item))
}

func (q *PriorityQueue) Pop() interface{} {
	old := q
	n := len(old.items)
	item := old.items[n-1]
	q.items = old.items[:n-1]
	return item
}
