package model

type ConsumerPartition struct {
	Group      string
	Topic      string
	Partition  int
	InstanceID string
}
