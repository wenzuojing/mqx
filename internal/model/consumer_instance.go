package model

import "time"

type ConsumerInstance struct {
	Group      string
	Topic      string
	InstanceID string
	Hostname   string
	Active     bool
	Heartbeat  time.Time
}
