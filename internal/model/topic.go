package model

type TopicMeta struct {
	Topic         string `json:"topic"`
	PartitionNum  int    `json:"partitionNum"`
	RetentionDays int    `json:"retentionDays"`
}
