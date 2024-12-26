package model

import "time"

type ConsumerInstance struct {
	Group      string    `json:"group"`
	Topic      string    `json:"topic"`
	InstanceID string    `json:"instanceId"`
	Hostname   string    `json:"hostname"`
	Active     bool      `json:"active"`
	Heartbeat  time.Time `json:"heartbeat"`
}
