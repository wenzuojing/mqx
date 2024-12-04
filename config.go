package mqx

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
}

// NewConfig creates a new Config with default values
func NewConfig() *Config {
	return &Config{
		DSN:                               "root:root@tcp(127.0.0.1:3306)/mqx?charset=utf8mb4&parseTime=True&loc=Local",
		DefaultPartitionNum:               8,
		PollingInterval:                   time.Second,
		PollingSize:                       100,
		RetentionDays:                     7,
		RebalanceInterval:                 time.Second * 30,
		RefreshConsumerPartitionsInterval: time.Second * 30,
		HeartbeatInterval:                 time.Second * 30,
		DelayInterval:                     time.Second * 5,
		PullingInterval:                   time.Second * 2,
		PullingSize:                       100,
		RetryInterval:                     time.Second * 3,
		RetryTimes:                        3,
	}
}

// WithDSN sets the database connection string
func (c *Config) WithDSN(dsn string) *Config {
	c.DSN = dsn
	return c
}

// WithDefaultPartitionNum sets the default number of partitions
func (c *Config) WithDefaultPartitionNum(num int) *Config {
	c.DefaultPartitionNum = num
	return c
}

// WithPollingInterval sets the message polling interval
func (c *Config) WithPollingInterval(interval time.Duration) *Config {
	c.PollingInterval = interval
	return c
}

// WithPollingSize sets the number of messages to poll
func (c *Config) WithPollingSize(size int) *Config {
	c.PollingSize = size
	return c
}

// WithRetentionTime sets the message retention period
func (c *Config) WithRetentionTime(duration time.Duration) *Config {
	c.RetentionTime = duration
	return c
}

// WithRebalanceInterval sets the consumer rebalance interval
func (c *Config) WithRebalanceInterval(interval time.Duration) *Config {
	c.RebalanceInterval = interval
	return c
}

// WithHeartbeatInterval sets the consumer heartbeat interval
func (c *Config) WithHeartbeatInterval(interval time.Duration) *Config {
	c.HeartbeatInterval = interval
	return c
}

// WithDelayInterval sets the delay message processing interval
func (c *Config) WithDelayInterval(interval time.Duration) *Config {
	c.DelayInterval = interval
	return c
}

// WithPullingInterval sets the message pulling interval
func (c *Config) WithPullingInterval(interval time.Duration) *Config {
	c.PullingInterval = interval
	return c
}

// WithPullingSize sets the batch size for message pulling
func (c *Config) WithPullingSize(size int) *Config {
	c.PullingSize = size
	return c
}

// WithRetryInterval sets the retry interval for failed operations
func (c *Config) WithRetryInterval(interval time.Duration) *Config {
	c.RetryInterval = interval
	return c
}

// WithRetryTimes sets the maximum number of retry attempts
func (c *Config) WithRetryTimes(times int) *Config {
	c.RetryTimes = times
	return c
}
