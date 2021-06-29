package main

type Queue struct {
	ch map[string]chan string
}

func NewQueue() *Queue {
	return &Queue{
		ch: make(map[string]chan string),
	}
}
