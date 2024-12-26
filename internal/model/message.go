package model

import "time"

type Message struct {
	MessageID string        `json:"messageId"`
	BornTime  time.Time     `json:"bornTime"`
	Topic     string        `json:"topic"`
	Key       string        `json:"key"`
	Tag       string        `json:"tag"`
	Body      []byte        `json:"body"`
	Partition int           `json:"partition"`
	Offset    int64         `json:"offset"`
	Delay     time.Duration `json:"delay"`
}

type DelayMessage struct {
	ID int64 `json:"id"`
	Message
}
