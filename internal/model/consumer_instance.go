package model

import "time"

type ConsumerInstance struct {
	GroupID    string
	InstanceID string
	Hostname   string
	Active     bool
	Heartbeat  time.Time
}
