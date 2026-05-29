package message

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/wenzuojing/mqx/internal/interfaces"
	"github.com/wenzuojing/mqx/internal/model"
	"github.com/wenzuojing/mqx/internal/template"
	"github.com/wenzuojing/mqx/pkg/templatex"
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

// prepareMessage validates topic, calculates partition, and assigns messageID.
// Shared by SaveMessage and SaveMessageWithTx.
func (s *messageManagerImpl) prepareMessage(msg *model.Message) error {
	if !isValidTopicName(msg.Topic) {
		return fmt.Errorf("invalid topic name")
	}
	if len(msg.Topic) > 256 {
		return fmt.Errorf("topic name '%s' is too long: maximum length is 512 characters", msg.Topic)
	}
	topicMeta, err := s.factory.GetTopicManager().GetTopicMeta(context.Background(), msg.Topic)
	if err != nil {
		return errors.Wrap(err, "failed to get topic metadata")
	}
	msg.Partition = s.calculatePartition(msg.Key, topicMeta.PartitionNum)
	if msg.MessageID == "" {
		msg.MessageID = uuid.New().String()
	}
	return nil
}

func (s *messageManagerImpl) SaveMessage(ctx context.Context, msg *model.Message) (string, error) {
	klog.V(4).Infof("Saving message to topic %s, key: %s", msg.Topic, msg.Key)

	if err := s.prepareMessage(msg); err != nil {
		return "", err
	}
	klog.V(4).Infof("Calculated partition %d for message", msg.Partition)

	tx, err := s.db.Begin()
	if err != nil {
		return "", errors.Wrap(err, "failed to begin transaction")
	}
	defer tx.Rollback()

	err = s.insertMessage(tx, msg)
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") {
			if err := s.createMessageTable(msg.Topic, msg.Partition); err != nil {
				return "", errors.Wrap(err, "failed to create message table")
			}
			err = s.insertMessage(tx, msg)
		}
		if err != nil {
			return "", errors.Wrap(err, "failed to insert message")
		}
	}

	if err = tx.Commit(); err != nil {
		return "", errors.Wrap(err, "failed to commit transaction")
	}
	klog.V(4).Infof("Successfully saved message with ID %s", msg.MessageID)
	return msg.MessageID, nil
}

// SaveMessageWithTx saves a message using a caller-managed transaction.
// The caller is responsible for committing or rolling back the transaction.
// IMPORTANT: This method does NOT create the message table automatically (DDL causes implicit commit in MySQL,
// which would break the caller's transaction atomicity). If the table doesn't exist, caller must create it
// outside the transaction first.
func (s *messageManagerImpl) SaveMessageWithTx(ctx context.Context, tx *sql.Tx, msg *model.Message) error {
	if err := s.prepareMessage(msg); err != nil {
		return err
	}
	if err := s.insertMessage(tx, msg); err != nil {
		return errors.Wrap(err, "failed to insert message")
	}
	return nil
}

// SaveRetryMessageWithTx saves a retry message with retry_count to a specific partition using a caller-managed transaction.
func (s *messageManagerImpl) SaveRetryMessageWithTx(ctx context.Context, tx *sql.Tx, topic string, partition int, msg *model.Message, retryCount int) error {
	if msg.MessageID == "" {
		msg.MessageID = uuid.New().String()
	}
	msg.Topic = topic
	msg.Partition = partition

	tableName := s.getMessageTableName(topic, partition)
	stmt, err := tx.Prepare(fmt.Sprintf(template.InsertMessageTemplate, tableName))
	if err != nil {
		return errors.Wrap(err, "failed to prepare statement")
	}
	defer stmt.Close()

	result, err := stmt.Exec(msg.MessageID, msg.Tag, msg.Key, msg.Body, msg.BornTime, retryCount)
	if err != nil {
		return errors.Wrap(err, "failed to insert retry message")
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to get rows affected")
	}
	if rowsAffected == 0 {
		return fmt.Errorf("retry message save failed")
	}

	return nil
}

func (s *messageManagerImpl) GetMessages(ctx context.Context, topic string, group string, partition int, offset int64, size int) ([]*model.Message, error) {
	klog.V(4).Infof("Getting messages from topic %s, partition %d, offset %d, size %d", topic, partition, offset, size)
	messages := make([]*model.Message, 0)
	rows, err := s.db.Query(fmt.Sprintf(template.SelectMessagesTemplate, s.getMessageTableName(topic, partition)), offset, size)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query messages")
	}
	defer rows.Close()

	for rows.Next() {
		var message model.Message
		message.Partition = partition
		err = rows.Scan(&message.MessageID, &message.Tag, &message.Key, &message.Body, &message.BornTime, &message.Offset, &message.RetryCount)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan message row")
		}
		messages = append(messages, &message)
	}
	klog.V(4).Infof("Retrieved %d messages", len(messages))
	return messages, nil
}

func (s *messageManagerImpl) GetMaxOffset(ctx context.Context, topic string, partition int) (int64, error) {
	klog.V(4).Infof("Getting max offset for topic %s, partition %d", topic, partition)
	var maxOffset int64
	err := s.db.QueryRow(fmt.Sprintf(template.SelectMaxOffsetTemplate, s.getMessageTableName(topic, partition)),
		partition).Scan(&maxOffset)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get max offset")
	}
	klog.V(4).Infof("Max offset is %d", maxOffset)
	return maxOffset, nil
}

func (s *messageManagerImpl) GetPartitionStat(ctx context.Context, topic string, partition int) (*interfaces.PartitionStat, error) {
	tableName := s.getMessageTableName(topic, partition)
	var stat interfaces.PartitionStat
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(template.SelectPartitionStatTemplate, tableName)).Scan(&stat.MaxOffset, &stat.MinOffset, &stat.Total)
	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") {
			return &stat, nil
		}
		return nil, err
	}
	return &stat, nil
}

func (s *messageManagerImpl) QueryMessageForPage(ctx context.Context, topic string, partition int, messageID string, tag string, pageNo int, pageSize int) (int, []*model.Message, error) {
	tableName := s.getMessageTableName(topic, partition)
	offset := (pageNo - 1) * pageSize
	limit := pageSize
	sql, err := templatex.Rander(template.SelectMessages2Template, map[string]interface{}{
		"TableName": tableName,
		"MessageID": messageID,
		"Tag":       tag,
	})
	if err != nil {
		return 0, nil, errors.Wrap(err, "failed to template sql")
	}

	args := []any{}
	if messageID != "" {
		args = append(args, messageID)
	}
	if tag != "" {
		args = append(args, tag)
	}
	args = append(args, limit, offset)

	rows, err := s.db.Query(sql, args...)

	if err != nil && strings.Contains(err.Error(), "doesn't exist") {
		return 0, nil, nil
	}

	if err != nil {
		return 0, nil, errors.Wrap(err, "failed to query messages")
	}
	defer rows.Close()
	messages := make([]*model.Message, 0)
	for rows.Next() {
		var message model.Message
		err = rows.Scan(&message.MessageID, &message.Tag, &message.Key, &message.Body, &message.BornTime, &message.Offset, &message.RetryCount)
		if err != nil {
			return 0, nil, errors.Wrap(err, "failed to scan message row")
		}
		messages = append(messages, &message)
	}
	return len(messages), messages, nil
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

// createMessageTable creates a new message table for a topic. Must be called outside a transaction (DDL causes implicit commit).
func (t *messageManagerImpl) createMessageTable(topic string, partition int) error {
	klog.V(4).Infof("Creating message table for topic %s, partition %d", topic, partition)
	_, err := t.db.Exec(fmt.Sprintf(template.CreateMessageTableTemplate, t.getMessageTableName(topic, partition)))
	if err != nil {
		return errors.Wrap(err, "failed to create message table")
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

	result, err := stmt.Exec(msg.MessageID, msg.Tag, msg.Key, msg.Body, msg.BornTime, msg.RetryCount)
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

func (s *messageManagerImpl) DeleteMessages(ctx context.Context, topic string, partition int) error {
	//drop table
	_, err := s.db.Exec(fmt.Sprintf(template.DropMessageTableTemplate, s.getMessageTableName(topic, partition)))
	if err != nil {
		return err
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
