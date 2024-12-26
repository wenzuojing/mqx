package interfaces

type PartitionStat struct {
	MaxOffset int64 `json:"maxOffset"`
	MinOffset int64 `json:"minOffset"`
	Total     int64 `json:"total"`
}
