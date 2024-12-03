package model

import "time"

type Message struct {
	MessageID string
	BornTime  time.Time
	Topic     string
	Key       string
	Tag       string
	Body      []byte
	Partition int
	Offset    int64
	Delay     time.Duration
}

type DelayMessage struct {
	ID int64
	Message
}
