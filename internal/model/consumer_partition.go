package model

type ConsumerOffset struct {
	Group      string `json:"group"`
	Topic      string `json:"topic"`
	Partition  int    `json:"partition"`
	Offset     int64  `json:"offset"`
	InstanceID string `json:"instanceId"`
}
