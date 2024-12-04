package message

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/wenzuojing/mqx/internal/interfaces"
	"github.com/wenzuojing/mqx/internal/model"
	"github.com/wenzuojing/mqx/internal/template"
	"k8s.io/klog/v2"
)

// MessageManager implements message storage and retrieval functionality
func NewMessageManager(db *sql.DB, factory interfaces.Factory) (interfaces.MessageManager, error) {
	return &messageManagerImpl{db: db, factory: factory}, nil
}

type messageManagerImpl struct {
	db      *sql.DB
	factory interfaces.Factory
}

func (s *messageManagerImpl) Start(ctx context.Context) error {
	klog.Info("Starting MessageManager service...")
	klog.Info("MessageManager service started successfully")
	return nil
}

func (s *messageManagerImpl) Stop(ctx context.Context) error {
	klog.Info("Stopping MessageManager service...")
	klog.Info("MessageManager service stopped successfully")
	return nil
}

func (s *messageManagerImpl) SaveMessage(ctx context.Context, msg *model.Message) (string, error) {
	klog.V(4).Infof("Saving message to topic %s, key: %s", msg.Topic, msg.Key)

	// Validate topic name - only allow alphanumeric and underscore characters
	if !isValidTopicName(msg.Topic) {
		err := fmt.Errorf("invalid topic name '%s': only alphanumeric and underscore characters are allowed", msg.Topic)
		klog.Error(err)
		return "", err
	}
	if len(msg.Topic) > 256 {
		return "", fmt.Errorf("topic name '%s' is too long: maximum length is 512 characters", msg.Topic)
	}

	topicMeta, err := s.factory.GetTopicManager().GetTopicMeta(ctx, msg.Topic)
	if err != nil {
		klog.Errorf("Failed to get topic metadata: %v", err)
		return "", err
	}
	// Calculate partition based on message key
	partition := s.calculatePartition(msg.Key, topicMeta.PartitionNum)
	msg.Partition = partition
	klog.V(4).Infof("Calculated partition %d for message", partition)

	tx, err := s.db.Begin()
	if err != nil {
		klog.Errorf("Failed to begin transaction: %v", err)
		return "", err
	}
	defer tx.Rollback()

	if msg.MessageID == "" {
		msg.MessageID = uuid.New().String()
	}

	err = s.insertMessage(tx, msg)
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") {
			if err := s.createMessageTable(msg.Topic, msg.Partition); err != nil {
				klog.Errorf("Failed to create message table: %v", err)
				return "", err
			}
			// Retry insert after table creation
			err = s.insertMessage(tx, msg)
		}
		if err != nil {
			klog.Errorf("Failed to insert message: %v", err)
			return "", err
		}
	}

	if err = tx.Commit(); err != nil {
		klog.Errorf("Failed to commit transaction: %v", err)
		return "", err
	}
	klog.V(4).Infof("Successfully saved message with ID %s", msg.MessageID)
	return msg.MessageID, nil
}

func (s *messageManagerImpl) GetMessages(ctx context.Context, topic string, group string, partition int, offset int64, size int) ([]*model.Message, error) {
	klog.V(4).Infof("Getting messages from topic %s, partition %d, offset %d, size %d", topic, partition, offset, size)
	messages := make([]*model.Message, 0)
	rows, err := s.db.Query(fmt.Sprintf(template.GetMessagesTemplate, s.getMessageTableName(topic, partition)), offset, size)
	if err != nil {
		klog.Errorf("Failed to query messages: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var message model.Message
		message.Partition = partition
		err = rows.Scan(&message.MessageID, &message.Tag, &message.Key, &message.Body, &message.BornTime, &message.Offset)
		if err != nil {
			klog.Errorf("Failed to scan message row: %v", err)
			return nil, err
		}
		messages = append(messages, &message)
	}
	klog.V(4).Infof("Retrieved %d messages", len(messages))
	return messages, nil
}

func (s *messageManagerImpl) GetMaxOffset(ctx context.Context, topic string, partition int) (int64, error) {
	klog.V(4).Infof("Getting max offset for topic %s, partition %d", topic, partition)
	var maxOffset int64
	err := s.db.QueryRow(fmt.Sprintf(template.GetMaxOffsetTemplate, s.getMessageTableName(topic, partition)),
		partition).Scan(&maxOffset)
	if err != nil {
		klog.Errorf("Failed to get max offset: %v", err)
		return 0, err
	}
	klog.V(4).Infof("Max offset is %d", maxOffset)
	return maxOffset, nil
}

func (s *messageManagerImpl) GetMessageTotal(ctx context.Context, topic string, partition int) (int64, error) {
	tableName := s.getMessageTableName(topic, partition)
	var total int64
	err := s.db.QueryRowContext(ctx, template.GetMessageTotalTemplate, tableName).Scan(&total)
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") {
			return 0, nil
		}
		return 0, err
	}
	return total, nil
}

// getMessageTableName returns the table name for a given topic
func (s *messageManagerImpl) getMessageTableName(topic string, partition int) string {
	return fmt.Sprintf("mqx_messages_%s_%d", topic, partition)
}

// calculatePartition determines the partition for a message based on its key
func (t *messageManagerImpl) calculatePartition(key string, partitionNum int) int {
	if key == "" {
		return 0
	}
	hash := 0
	for _, c := range key {
		hash = 31*hash + int(c)
	}
	return abs(hash) % partitionNum
}

// createMessageTable creates a new message table for a topic
func (t *messageManagerImpl) createMessageTable(topic string, partition int) error {
	klog.V(4).Infof("Creating message table for topic %s, partition %d", topic, partition)
	_, err := t.db.Exec(fmt.Sprintf(template.CreateMessageTableTemplate, t.getMessageTableName(topic, partition)))
	if err != nil {
		klog.Errorf("Failed to create message table: %v", err)
		return err
	}
	klog.V(4).Info("Message table created successfully")
	return nil
}

// insertMessage inserts a message into the database
func (s *messageManagerImpl) insertMessage(tx *sql.Tx, msg *model.Message) error {
	stmt, err := tx.Prepare(fmt.Sprintf(template.InsertMessageTemplate, s.getMessageTableName(msg.Topic, msg.Partition)))
	if err != nil {
		return err
	}
	defer stmt.Close()

	result, err := stmt.Exec(msg.MessageID, msg.Tag, msg.Key, msg.Body, msg.BornTime)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return fmt.Errorf("message save failed")
	}
	return nil
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// isValidTopicName checks if topic name contains only alphanumeric and underscore characters
func isValidTopicName(topic string) bool {
	for _, c := range topic {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
			return false
		}
	}
	return true
}
