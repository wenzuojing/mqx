package model

type ConsumerPartition struct {
	Group      string
	Topic      string
	Partition  int
	Offset     int64
	InstanceID string
}
