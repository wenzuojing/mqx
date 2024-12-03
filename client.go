package mqx

import (
	"context"
	"time"

	"github.com/wenzuojing/mqx/internal"
	"github.com/wenzuojing/mqx/internal/config"
	"github.com/wenzuojing/mqx/internal/model"
)

// Message represents a message to be sent or received
type Message struct {
	Topic string        // Topic name for the message
	Key   string        // Optional key for message routing
	Tag   string        // Optional tag for message filtering
	Body  []byte        // Message payload
	Delay time.Duration // Optional delay duration for delayed messages
}

// NewMessage creates a new message instance with default values
func NewMessage() *Message {
	return &Message{}
}

// WithTopic sets the topic for the message
func (m *Message) WithTopic(topic string) *Message {
	m.Topic = topic
	return m
}

// WithKey sets the key for the message
func (m *Message) WithKey(key string) *Message {
	m.Key = key
	return m
}

// WithTag sets the tag for the message
func (m *Message) WithTag(tag string) *Message {
	m.Tag = tag
	return m
}

// WithBody sets the body for the message
func (m *Message) WithBody(body []byte) *Message {
	m.Body = body
	return m
}

// WithDelay sets the delay duration for the message
func (m *Message) WithDelay(delay time.Duration) *Message {
	m.Delay = delay
	return m
}

// MessageView represents a received message with additional metadata
type MessageView struct {
	MessageID string    // Unique message identifier
	BornTime  time.Time // Message creation timestamp
	Group     string    // Consumer group
	Topic     string    // Topic name
	Key       string    // Message routing key
	Tag       string    // Message tag
	Body      []byte    // Message payload
	Partition int       // Partition number where message is stored
}

// MessageHandler defines the callback function for message processing
type MessageHandler func(msg *MessageView) error

// MQX defines the main interface for message queue operations
type MQX interface {
	// SendSync sends a message synchronously and returns its ID
	SendSync(ctx context.Context, msg *Message) (string, error)
	// SendAsync sends a message asynchronously with a callback
	SendAsync(ctx context.Context, msg *Message, callback func(string, error)) error
	// GroupSubscribe creates a consumer group subscription
	GroupSubscribe(ctx context.Context, topic string, group string, handler MessageHandler) error
	// BroadcastSubscribe creates a broadcast subscription where each consumer receives all messages
	BroadcastSubscribe(ctx context.Context, topic string, handler MessageHandler) error
	// Close gracefully shuts down the message queue client
	Close(ctx context.Context) error
}

// NewMQX creates a new message queue instance with the provided configuration
func NewMQX(cfg *Config) (MQX, error) {
	messageService, err := internal.NewMessageService(&config.Config{
		DSN:                 cfg.DSN,
		DefaultPartitionNum: cfg.DefaultPartitionNum,
		PollingInterval:     cfg.PollingInterval,
		PollingSize:         cfg.PollingSize,
		HeartbeatInterval:   cfg.HeartbeatInterval,
		RebalanceInterval:   cfg.RebalanceInterval,
		DelayInterval:       cfg.DelayInterval,
		PullingInterval:     cfg.PullingInterval,
		PullingSize:         cfg.PullingSize,
		RetryInterval:       cfg.RetryInterval,
		RetryTimes:          cfg.RetryTimes,
		RetentionTime:       cfg.RetentionTime,
	})
	if err != nil {
		return nil, err
	}
	if err := messageService.Start(context.TODO()); err != nil {
		return nil, err
	}

	return &client{
		messageService: messageService,
	}, nil
}

// client implements the MQX interface
type client struct {
	messageService internal.MessageService
}

// SendSync sends a message synchronously
func (c *client) SendSync(ctx context.Context, msg *Message) (string, error) {
	// Convert Message to internal model.Message
	return c.messageService.SendSync(ctx, &model.Message{
		Topic:    msg.Topic,
		Key:      msg.Key,
		Tag:      msg.Tag,
		Body:     msg.Body,
		BornTime: time.Now(),
		Delay:    msg.Delay,
	})
}

// SendAsync sends a message asynchronously
func (c *client) SendAsync(ctx context.Context, msg *Message, callback func(string, error)) error {
	return c.messageService.SendAsync(ctx, &model.Message{
		Topic:    msg.Topic,
		Key:      msg.Key,
		Tag:      msg.Tag,
		Body:     msg.Body,
		BornTime: time.Now(),
		Delay:    msg.Delay,
	}, callback)
}

// GroupSubscribe creates a consumer group subscription
func (c *client) GroupSubscribe(ctx context.Context, topic string, group string, handler MessageHandler) error {
	return c.messageService.GroupSubscribe(ctx, topic, group, func(msg *model.Message) error {
		return handler(&MessageView{
			MessageID: msg.MessageID,
			Group:     group,
			Topic:     topic,
			BornTime:  msg.BornTime,
			Key:       msg.Key,
			Tag:       msg.Tag,
			Partition: msg.Partition,
			Body:      msg.Body,
		})
	})
}

// BroadcastSubscribe creates a broadcast subscription
func (c *client) BroadcastSubscribe(ctx context.Context, topic string, handler MessageHandler) error {
	return c.messageService.BroadcastSubscribe(ctx, topic, func(msg *model.Message) error {
		return handler(&MessageView{
			MessageID: msg.MessageID,
			BornTime:  msg.BornTime,
			Topic:     msg.Topic,
			Key:       msg.Key,
			Tag:       msg.Tag,
			Partition: msg.Partition,
		})
	})
}

// Close gracefully shuts down the message queue client
func (c *client) Close(ctx context.Context) error {
	return c.messageService.Stop(ctx)
}
