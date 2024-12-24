package interfaces

import (
	"context"

	"github.com/wenzuojing/mqx/internal/model"
)

// MessageManager handles message storage and retrieval operations
type MessageManager interface {
	// Start initializes the message manager service
	Start(ctx context.Context) error
	// Stop gracefully shuts down the message manager service
	Stop(ctx context.Context) error
	// SaveMessage persists a message to storage and returns its ID
	SaveMessage(ctx context.Context, msg *model.Message) (string, error)
	// GetMessages retrieves messages from a specific partition after the given offset
	GetMessages(ctx context.Context, topic string, group string, partition int, offset int64, size int) ([]*model.Message, error)
	// GetMaxOffset returns the highest offset in a partition
	GetMaxOffset(ctx context.Context, topic string, partition int) (int64, error)
	// GetMessageTotal returns the total number of messages in a partition
	GetMessageTotal(ctx context.Context, topic string, partition int) (int64, error)
	// DeleteMessages deletes messages from a specific partition
	DeleteMessages(ctx context.Context, topic string, partition int) error
}

// TopicManager handles topic metadata management
type TopicManager interface {
	// GetTopicMeta retrieves metadata for a specific topic
	GetTopicMeta(ctx context.Context, topic string) (*model.TopicMeta, error)
	// GetAllTopicMeta retrieves metadata for all topics
	GetAllTopicMeta(ctx context.Context) ([]model.TopicMeta, error)
	// UpdateTopicMeta updates the metadata for a topic
	UpdateTopicMeta(ctx context.Context, meta *model.TopicMeta) error
	// Start initializes the topic manager service
	Start(ctx context.Context) error
	// Stop gracefully shuts down the topic manager service
	Stop(ctx context.Context) error
	// CreateTopic creates a new topic
	CreateTopic(ctx context.Context, meta *model.TopicMeta) error
	// DeleteTopic deletes a topic
	DeleteTopic(ctx context.Context, topic string) error
}

// ConsumerManager handles message consumption and consumer group management
type ConsumerManager interface {
	// DeleteConsumerPartitions deletes the partitions by topic
	DeleteConsumerPartitions(ctx context.Context, topic string) error
	// GetConsumerPartitions returns the partitions by topic
	GetConsumerPartitions(ctx context.Context, topic string) ([]model.ConsumerPartition, error)
	// GetConsumerInstances returns the instances by topic
	GetConsumerInstances(ctx context.Context, topic string) ([]model.ConsumerInstance, error)
	// Consume starts consuming messages from a topic with the specified handler
	Consume(ctx context.Context, topic string, group string, handler func(msg *model.Message) error) error
	// Start initializes the consumer manager service
	Start(ctx context.Context) error
	// Stop gracefully shuts down the consumer manager service
	Stop(ctx context.Context) error
}

// ProducerManager handles message production and sending
type ProducerManager interface {
	// SendSync sends a message synchronously and returns its ID
	SendSync(ctx context.Context, msg *model.Message) (string, error)
	// SendAsync sends a message asynchronously with a callback for the result
	SendAsync(ctx context.Context, msg *model.Message, callback func(string, error)) error
	// Start initializes the producer manager service
	Start(ctx context.Context) error
	// Stop gracefully shuts down the producer manager service
	Stop(ctx context.Context) error
}

// DelayManager handles delayed message processing
type DelayManager interface {
	// Add adds a message to the delay queue
	Add(ctx context.Context, msg *model.Message) (string, error)
	// Start initializes the delay manager service
	Start(ctx context.Context) error
	// Stop gracefully shuts down the delay manager service
	Stop(ctx context.Context) error
}

type ClearManager interface {
	// Start initializes the clear manager service
	Start(ctx context.Context) error
	// Stop gracefully shuts down the clear manager service
	Stop(ctx context.Context) error
}

// Factory provides access to various manager instances
type Factory interface {
	// GetTopicManager returns the topic manager instance
	GetTopicManager() TopicManager
	// GetMessageManager returns the message manager instance
	GetMessageManager() MessageManager
	// GetConsumerManager returns the consumer manager instance
	GetConsumerManager() ConsumerManager
	// GetProducerManager returns the producer manager instance
	GetProducerManager() ProducerManager
	// GetDelayManager returns the delay manager instance
	GetDelayManager() DelayManager
	// GetClearManager returns the clear manager instance
	GetClearManager() ClearManager
}
