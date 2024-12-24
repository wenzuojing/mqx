package config

import "time"

type Config struct {
	DSN                               string        // Database connection string
	DefaultPartitionNum               int           // Default number of partitions
	PollingInterval                   time.Duration // Message polling interval
	PollingSize                       int           // Number of messages to poll
	RetentionDays                     int           // Message retention days
	RebalanceInterval                 time.Duration // Consumer rebalance interval
	RefreshConsumerPartitionsInterval time.Duration // Refresh consumer partitions interval
	HeartbeatInterval                 time.Duration // Consumer heartbeat interval
	DelayInterval                     time.Duration // Delay message processing interval
	PullingInterval                   time.Duration // Message pulling interval
	PullingSize                       int           // Batch size for message pulling
	RetryInterval                     time.Duration // Retry interval for failed operations
	RetryTimes                        int           // Maximum number of retry attempts
	ClearInterval                     time.Duration // Clear interval for expired messages
	Console                           Console       // Console configuration
}

type Console struct {
	Address string // Console server address
}
