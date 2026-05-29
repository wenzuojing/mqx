# Async Retry via Delay Queue - Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace blocking inline retry mechanism with async delay queue-based retries to prevent partition consumption from being blocked during retry waits.

**Architecture:** When a message handler fails, the message is immediately sent to the delay queue with retry metadata. The consumer offset advances, unblocking the partition. The delay manager transfers retry messages back to the original topic+partition after exponential backoff. Max retries exhausted → DLQ.

**Tech Stack:** Go, MySQL, existing delay queue infrastructure

---

## File Structure

**Models:**
- Modify: `internal/model/message.go` - Add RetryCount field to Message, extend DelayMessage, add RetryMessage type

**SQL Templates:**
- Modify: `internal/template/sql/delay/create_delay_message_table.sql` - Add retry_count, original_group, original_partition columns
- Modify: `internal/template/sql/delay/insert_delay_message.sql` - Include new columns in INSERT
- Modify: `internal/template/sql/delay/get_ready_delay_messages.sql` - Select new columns
- Modify: `internal/template/sql/message/create_message_table.sql` - Add retry_count column
- Modify: `internal/template/sql/message/insert_message.sql` - Include retry_count in INSERT
- Modify: `internal/template/sql/message/select_messages.sql` - Select retry_count

**Interfaces:**
- Modify: `internal/interfaces/interfaces.go:82-91` - Add AddRetry to DelayManager interface
- Modify: `internal/interfaces/interfaces.go:11-31` - Add SaveRetryMessageWithTx to MessageManager interface

**Implementations:**
- Modify: `internal/delay/delay_manager.go` - Implement AddRetry, transferRetryMessage, distinguish retry vs user delay in processDelayMessages
- Modify: `internal/message/message_manager.go` - Implement SaveRetryMessageWithTx
- Modify: `internal/consumer/partition_consumer.go:45-141` - Remove blocking callHandler retry loop, integrate delay queue for retries

**Tests:**
- Modify: `internal/consumer/partition_consumer_test.go` - Update callHandler tests, add consume retry tests
- Create: `internal/delay/delay_manager_test.go` - Test AddRetry and transferRetryMessage
- Modify: `internal/message/message_manager_test.go` - Test SaveRetryMessageWithTx

---

### Task 1: Update Message Models

**Files:**
- Modify: `internal/model/message.go`

- [ ] **Step 1: Add RetryCount to Message struct**

```go
type Message struct {
	MessageID  string        `json:"messageId"`
	BornTime   time.Time     `json:"bornTime"`
	Topic      string        `json:"topic"`
	Key        string        `json:"key"`
	Tag        string        `json:"tag"`
	Body       []byte        `json:"body"`
	Partition  int           `json:"partition"`
	Offset     int64         `json:"offset"`
	Delay      time.Duration `json:"delay"`
	RetryCount int           `json:"retryCount"` // NEW
}
```

- [ ] **Step 2: Extend DelayMessage with retry fields**

```go
type DelayMessage struct {
	ID                int64 `json:"id"`
	Message
	RetryCount        int    `json:"retryCount"`        // NEW
	OriginalGroup     string `json:"originalGroup"`     // NEW
	OriginalPartition int    `json:"originalPartition"` // NEW
}
```

- [ ] **Step 3: Add RetryMessage type**

```go
type RetryMessage struct {
	Message
	RetryCount        int
	OriginalGroup     string
	OriginalPartition int
	Delay             time.Duration
}
```

- [ ] **Step 4: Commit**

```bash
git add internal/model/message.go
git commit -m "feat: add retry metadata fields to message models"
```

---

### Task 2: Update Delay Table Schema

**Files:**
- Modify: `internal/template/sql/delay/create_delay_message_table.sql`
- Modify: `internal/template/sql/delay/insert_delay_message.sql`
- Modify: `internal/template/sql/delay/get_ready_delay_messages.sql`

- [ ] **Step 1: Update CREATE TABLE to include new columns**

Replace `internal/template/sql/delay/create_delay_message_table.sql`:

```sql
CREATE TABLE IF NOT EXISTS mqx_delay_messages (
    `id` BIGINT PRIMARY KEY AUTO_INCREMENT,
    `message_id` VARCHAR(64),
    `topic` VARCHAR(256) NOT NULL,
    `key` VARCHAR(256),
    `tag` VARCHAR(256),
    `body` BLOB NOT NULL,
    `born_time` DATETIME NOT NULL,
    `delay_time` DATETIME NOT NULL,
    `retry_count` INT NOT NULL DEFAULT 0,
    `original_group` VARCHAR(256) DEFAULT NULL,
    `original_partition` INT DEFAULT NULL,
    INDEX `idx_delay_time` (`delay_time`)
) ENGINE=InnoDB;
```

- [ ] **Step 2: Update INSERT to include new columns**

Replace `internal/template/sql/delay/insert_delay_message.sql`:

```sql
INSERT INTO mqx_delay_messages (
    `message_id`,
    `topic`,
    `key`,
    `tag`,
    `body`,
    `born_time`,
    `delay_time`,
    `retry_count`,
    `original_group`,
    `original_partition`
) VALUES (
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?
);
```

- [ ] **Step 3: Update SELECT to include new columns**

Replace `internal/template/sql/delay/get_ready_delay_messages.sql`:

```sql
SELECT 
    `id`,
    `message_id`,
    `topic`,
    `key`,
    `tag`,
    `body`,
    `born_time`,
    `delay_time`,
    `retry_count`,
    `original_group`,
    `original_partition`
FROM mqx_delay_messages 
WHERE `delay_time` <= NOW()
ORDER BY `born_time` ASC
LIMIT 100;
```

- [ ] **Step 4: Commit**

```bash
git add internal/template/sql/delay/
git commit -m "feat: extend delay table schema for retry tracking"
```

---

### Task 3: Update Message Table Schema

**Files:**
- Modify: `internal/template/sql/message/create_message_table.sql`
- Modify: `internal/template/sql/message/insert_message.sql`
- Modify: `internal/template/sql/message/select_messages.sql`

- [ ] **Step 1: Update CREATE TABLE to include retry_count**

Replace `internal/template/sql/message/create_message_table.sql`:

```sql
CREATE TABLE IF NOT EXISTS `%s` (
    `offset` BIGINT PRIMARY KEY AUTO_INCREMENT,
    `message_id` VARCHAR(64),
    `tag` VARCHAR(256),
    `key` VARCHAR(256),
    `body` BLOB,
    `born_time` DATETIME NOT NULL,
    `retry_count` INT NOT NULL DEFAULT 0,
    UNIQUE KEY `uk_message_id` (`message_id`),
    KEY `idx_tag` (`tag`)
) ENGINE = InnoDB
```

- [ ] **Step 2: Update INSERT to include retry_count**

Replace `internal/template/sql/message/insert_message.sql`:

```sql
INSERT INTO `%s` (
    `message_id`,
    `tag`,
    `key`,
    `body`,
    `born_time`,
    `retry_count`
) VALUES (
    ?,
    ?,
    ?,
    ?,
    ?,
    ?
)
```

- [ ] **Step 3: Update SELECT to include retry_count**

Replace `internal/template/sql/message/select_messages.sql`:

```sql
SELECT 
    `message_id`, 
    `tag`, 
    `key`, 
    `body`, 
    `born_time`, 
    `offset`,
    `retry_count`
FROM `%s` 
WHERE `offset` > ? 
ORDER BY `offset` ASC 
LIMIT ?
```

- [ ] **Step 4: Commit**

```bash
git add internal/template/sql/message/
git commit -m "feat: add retry_count to message table schema"
```

---

### Task 4: Update Interfaces

**Files:**
- Modify: `internal/interfaces/interfaces.go`

- [ ] **Step 1: Add AddRetry to DelayManager interface**

At line 82 in `internal/interfaces/interfaces.go`, update the DelayManager interface:

```go
// DelayManager handles delayed message processing
type DelayManager interface {
	// Add adds a message to the delay queue
	Add(ctx context.Context, msg *model.Message) (string, error)
	// AddRetry adds a failed message to the delay queue for async retry
	AddRetry(ctx context.Context, msg *model.RetryMessage) (string, error)
	// DeleteMessagesByTopic deletes all delayed messages for a topic
	DeleteMessagesByTopic(ctx context.Context, topic string) error
	// Start initializes the delay manager service
	Start(ctx context.Context) error
	// Stop gracefully shuts down the delay manager service
	Stop(ctx context.Context) error
}
```

- [ ] **Step 2: Add SaveRetryMessageWithTx to MessageManager interface**

At line 11 in `internal/interfaces/interfaces.go`, update the MessageManager interface:

```go
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
	// GetPartitionStat returns the stat of messages in a partition
	GetPartitionStat(ctx context.Context, topic string, partition int) (*PartitionStat, error)
	// DeleteMessages deletes messages from a specific partition
	DeleteMessages(ctx context.Context, topic string, partition int) error
	// QueryMessageForPage retrieves messages from a specific partition for pagination
	QueryMessageForPage(ctx context.Context, topic string, partition int, messageID string, tag string, pageNo int, pageSize int) (int, []*model.Message, error)
	// SaveMessageWithTx saves a message using a caller-managed transaction.
	// The caller is responsible for committing or rolling back the transaction.
	SaveMessageWithTx(ctx context.Context, tx *sql.Tx, msg *model.Message) error
	// SaveRetryMessageWithTx saves a retry message with retry_count to a specific partition
	SaveRetryMessageWithTx(ctx context.Context, tx *sql.Tx, topic string, partition int, msg *model.Message, retryCount int) error
}
```

- [ ] **Step 3: Commit**

```bash
git add internal/interfaces/interfaces.go
git commit -m "feat: add AddRetry and SaveRetryMessageWithTx to interfaces"
```

---

### Task 5: Implement SaveRetryMessageWithTx in MessageManager

**Files:**
- Modify: `internal/message/message_manager.go`
- Test: `internal/message/message_manager_test.go`

- [ ] **Step 1: Write test for SaveRetryMessageWithTx**

Add to `internal/message/message_manager_test.go`:

```go
func TestMessageManager_SaveRetryMessageWithTx(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mockFactory := new(MockFactory)
	mockTopicManager := new(MockTopicManager)
	mockFactory.On("GetTopicManager").Return(mockTopicManager)

	mockTopicManager.On("GetTopicMeta", mock.Anything, "test-topic").
		Return(&model.TopicMeta{Name: "test-topic", PartitionNum: 4}, nil)

	mm := &messageManagerImpl{db: db, factory: mockFactory}

	msg := &model.Message{
		MessageID: "retry-msg-1",
		Topic:     "test-topic",
		Key:       "key1",
		Tag:       "tag1",
		Body:      []byte("retry body"),
		BornTime:  time.Now(),
	}

	mock.ExpectPrepare("INSERT INTO `mqx_messages_test-topic_1`").
		ExpectExec().
		WithArgs("retry-msg-1", "tag1", "key1", []byte("retry body"), sqlmock.AnyArg(), 2).
		WillReturnResult(sqlmock.NewResult(1, 1))

	tx, _ := db.Begin()
	err = mm.SaveRetryMessageWithTx(context.Background(), tx, "test-topic", 1, msg, 2)
	assert.NoError(t, err)
	tx.Rollback()

	assert.NoError(t, mock.ExpectationsWereMet())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/wens/wend-dev/mqx && go test ./internal/message -run TestMessageManager_SaveRetryMessageWithTx -v`

Expected: FAIL - method not implemented

- [ ] **Step 3: Implement SaveRetryMessageWithTx**

Add to `internal/message/message_manager.go` after line 107:

```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/wens/wend-dev/mqx && go test ./internal/message -run TestMessageManager_SaveRetryMessageWithTx -v`

Expected: PASS

- [ ] **Step 5: Update insertMessage to include retry_count**

Modify `internal/message/message_manager.go` line 236:

```go
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
```

- [ ] **Step 6: Update GetMessages to scan retry_count**

Modify `internal/message/message_manager.go` line 121:

```go
for rows.Next() {
	var message model.Message
	message.Partition = partition
	err = rows.Scan(&message.MessageID, &message.Tag, &message.Key, &message.Body, &message.BornTime, &message.Offset, &message.RetryCount)
	if err != nil {
		return nil, errors.Wrap(err, "failed to scan message row")
	}
	messages = append(messages, &message)
}
```

- [ ] **Step 7: Update QueryMessageForPage to scan retry_count**

Modify `internal/message/message_manager.go` line 191:

```go
for rows.Next() {
	var message model.Message
	err = rows.Scan(&message.MessageID, &message.Tag, &message.Key, &message.Body, &message.BornTime, &message.Offset, &message.RetryCount)
	if err != nil {
		return 0, nil, errors.Wrap(err, "failed to scan message row")
	}
	messages = append(messages, &message)
}
```

- [ ] **Step 8: Run all message manager tests**

Run: `cd /Users/wens/wend-dev/mqx && go test ./internal/message -v`

Expected: All tests PASS

- [ ] **Step 9: Commit**

```bash
git add internal/message/
git commit -m "feat: implement SaveRetryMessageWithTx and update message scanning"
```

---

### Task 6: Implement AddRetry in DelayManager

**Files:**
- Modify: `internal/delay/delay_manager.go`
- Create: `internal/delay/delay_manager_test.go`

- [ ] **Step 1: Write test for AddRetry**

Create `internal/delay/delay_manager_test.go`:

```go
package delay

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/wenzuojing/mqx/internal/model"
)

func TestDelayManager_AddRetry(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	dm := &delayManagerImpl{db: db, stopChan: make(chan struct{})}

	retryMsg := &model.RetryMessage{
		Message: model.Message{
			MessageID: "retry-msg-1",
			Topic:     "test-topic",
			Key:       "key1",
			Tag:       "tag1",
			Body:      []byte("retry body"),
			BornTime:  time.Now(),
		},
		RetryCount:        2,
		OriginalGroup:     "test-group",
		OriginalPartition: 1,
		Delay:             time.Second * 10,
	}

	mock.ExpectExec("INSERT INTO mqx_delay_messages").
		WithArgs(
			"retry-msg-1",
			"test-topic",
			"key1",
			"tag1",
			[]byte("retry body"),
			sqlmock.AnyArg(), // bornTime
			sqlmock.AnyArg(), // delayTime
			2,
			"test-group",
			1,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	msgID, err := dm.AddRetry(context.Background(), retryMsg)
	assert.NoError(t, err)
	assert.Equal(t, "retry-msg-1", msgID)

	assert.NoError(t, mock.ExpectationsWereMet())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/wens/wend-dev/mqx && go test ./internal/delay -run TestDelayManager_AddRetry -v`

Expected: FAIL - method not implemented

- [ ] **Step 3: Implement AddRetry**

Add to `internal/delay/delay_manager.go` after line 50:

```go
func (d *delayManagerImpl) AddRetry(ctx context.Context, msg *model.RetryMessage) (string, error) {
	klog.V(4).Infof("Adding retry message for topic: %s, delay: %v, retryCount: %d", msg.Topic, msg.Delay, msg.RetryCount)
	if msg.MessageID == "" {
		msg.MessageID = uuid.New().String()
	}
	delayTime := time.Now().Add(msg.Delay)
	_, err := d.db.Exec(template.InsertDelayMessage,
		msg.MessageID,
		msg.Topic,
		msg.Key,
		msg.Tag,
		msg.Body,
		msg.BornTime,
		delayTime,
		msg.RetryCount,
		msg.OriginalGroup,
		msg.OriginalPartition,
	)
	if err != nil {
		klog.Errorf("Failed to insert retry message: %v", err)
		return "", err
	}
	klog.V(4).Infof("Successfully added retry message with ID: %s", msg.MessageID)
	return msg.MessageID, nil
}
```

- [ ] **Step 4: Update existing Add method to pass new columns**

The INSERT SQL template now has 10 parameters. Update the existing `Add` method at line 30-50 to pass `nil` for the three new retry-specific columns (user-initiated delays are not retries):

```go
func (d *delayManagerImpl) Add(ctx context.Context, msg *model.Message) (string, error) {
	klog.V(4).Infof("Adding delayed message for topic: %s, delay: %v", msg.Topic, msg.Delay)
	if msg.MessageID == "" {
		msg.MessageID = uuid.New().String()
	}
	_, err := d.db.Exec(template.InsertDelayMessage,
		msg.MessageID,
		msg.Topic,
		msg.Key,
		msg.Tag,
		msg.Body,
		msg.BornTime,
		msg.BornTime.Add(msg.Delay),
		0,    // retry_count: user-initiated delays are not retries
		nil,  // original_group: not applicable
		nil,  // original_partition: not applicable
	)
	if err != nil {
		klog.Errorf("Failed to insert delayed message: %v", err)
		return "", err
	}
	klog.V(4).Infof("Successfully added delayed message with ID: %s", msg.MessageID)
	return msg.MessageID, nil
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd /Users/wens/wend-dev/mqx && go test ./internal/delay -run TestDelayManager_AddRetry -v`

Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/delay/
git commit -m "feat: implement AddRetry for delay manager"
```

---

### Task 7: Implement transferRetryMessage and Update processDelayMessages

**Files:**
- Modify: `internal/delay/delay_manager.go:88-247`

- [ ] **Step 1: Add transferRetryMessage method**

Add to `internal/delay/delay_manager.go` after AddRetry method:

```go
// transferRetryMessage moves a retry message back to its original topic and partition with retry_count
func (d *delayManagerImpl) transferRetryMessage(ctx context.Context, tx *sql.Tx, msg *model.DelayMessage) error {
	return d.factory.GetMessageManager().SaveRetryMessageWithTx(ctx, tx,
		msg.Topic, msg.OriginalPartition, &msg.Message, msg.RetryCount)
}
```

- [ ] **Step 2: Update processDelayMessages to scan new columns**

Modify `internal/delay/delay_manager.go` line 121 (inside the rows.Next loop):

```go
for rows.Next() {
	var msg model.DelayMessage
	var delayTime time.Time
	err := rows.Scan(&msg.ID, &msg.MessageID, &msg.Topic, &msg.Key, &msg.Tag, &msg.Body, &msg.BornTime, &delayTime,
		&msg.RetryCount, &msg.OriginalGroup, &msg.OriginalPartition)
	if err != nil {
		klog.Warningf("Failed to scan delayed message: %v", err)
		continue
	}
	messages = append(messages, &msg)
}
```

- [ ] **Step 3: Update processDelayMessages to distinguish retry vs user delay**

Modify `internal/delay/delay_manager.go` line 149 (inside the for loop over messages):

```go
for _, msg := range messages {
	if poisonPills[msg.MessageID] {
		continue
	}

	// Distinguish retry messages from user-initiated delay messages
	if msg.RetryCount > 0 {
		// Retry message -> transfer back to original topic+partition with retry_count
		err = d.transferRetryMessage(ctx, tx, msg)
	} else {
		// User-initiated delay -> existing behavior
		err = d.factory.GetMessageManager().SaveMessageWithTx(ctx, tx, &msg.Message)
	}

	if err != nil {
		if strings.Contains(err.Error(), "doesn't exist") {
			// DDL causes implicit commit in MySQL — must create table outside tx
			tx.Rollback()
			topicMeta, metaErr := d.factory.GetTopicManager().GetTopicMeta(ctx, msg.Topic)
			if metaErr != nil {
				klog.Errorf("Failed to get topic meta for table creation: %v", metaErr)
				poisonPills[msg.MessageID] = true
				// Start fresh tx for remaining messages
				tx, err = d.db.Begin()
				if err != nil {
					return err
				}
				continue
			}
			// For retry messages, use the original partition; for user delays, calculate it
			var partition int
			if msg.RetryCount > 0 {
				partition = msg.OriginalPartition
			} else {
				key := msg.Key
				hash := 0
				for _, c := range key {
					hash = 31*hash + int(c)
				}
				partition = hash % topicMeta.PartitionNum
				if partition < 0 {
					partition = -partition
				}
			}
			tableName := fmt.Sprintf("mqx_messages_%s_%d", msg.Topic, partition)
			if _, createErr := d.db.Exec(fmt.Sprintf(template.CreateMessageTableTemplate, tableName)); createErr != nil {
				klog.Errorf("Failed to create message table %s: %v", tableName, createErr)
				poisonPills[msg.MessageID] = true
				// Start fresh tx for remaining messages
				tx, err = d.db.Begin()
				if err != nil {
					return err
				}
				continue
			}
			// Retry with a fresh transaction
			tx, err = d.db.Begin()
			if err != nil {
				return err
			}
			// Re-attempt the transfer with the new table
			if msg.RetryCount > 0 {
				err = d.transferRetryMessage(ctx, tx, msg)
			} else {
				err = d.factory.GetMessageManager().SaveMessageWithTx(ctx, tx, &msg.Message)
			}
		}
		if err != nil {
			// Poison pill: record failed message ID and skip it to unblock remaining messages
			klog.Errorf("Poison pill detected — message %s failed to transfer, skipping: %v", msg.MessageID, err)
			poisonPills[msg.MessageID] = true
			// Start fresh tx for remaining messages (current tx may be poisoned)
			tx.Rollback()
			tx, err = d.db.Begin()
			if err != nil {
				return err
			}
			continue
		}
	}

	// Remove processed message from delay queue (within the same tx)
	_, err = tx.Exec(template.DeleteDelayMessage, msg.ID)
	if err != nil {
		klog.Errorf("Failed to delete processed delayed message: %v", err)
		tx.Rollback()
		return err
	}
}
```

- [ ] **Step 4: Run delay manager tests**

Run: `cd /Users/wens/wend-dev/mqx && go test ./internal/delay -v`

Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add internal/delay/
git commit -m "feat: implement transferRetryMessage and distinguish retry vs user delay"
```

---

### Task 8: Update partition_consumer to Use Async Retry

**Files:**
- Modify: `internal/consumer/partition_consumer.go:45-204`

- [ ] **Step 1: Simplify callHandler to remove retry loop**

Replace `internal/consumer/partition_consumer.go` line 173-204:

```go
// callHandler executes message handler once without retry.
// Retry logic is now handled asynchronously via the delay queue.
func (p *partitionConsumer) callHandler(msg *model.Message) error {
	return p.handler(msg)
}
```

- [ ] **Step 2: Update consume() to integrate delay queue for retries**

Replace `internal/consumer/partition_consumer.go` line 89-131 (the message processing loop):

```go
// Process fetched messages
for _, msg := range msgs {
	if err := p.callHandler(msg); err != nil {
		// Handler failed - decide between retry or DLQ
		if msg.RetryCount >= p.cfg.RetryTimes-1 {
			// Max retries exhausted -> dead letter queue
			klog.Errorf("Message %s exhausted retries (%d), sending to DLQ: %v",
				msg.MessageID, p.cfg.RetryTimes, err)
			_, dlqErr := p.factory.GetMessageManager().SaveMessage(ctx, &model.Message{
				MessageID: msg.MessageID,
				Topic:     msg.Topic + "_dead",
				Partition: msg.Partition,
				Key:       msg.Key,
				Tag:       msg.Tag,
				BornTime:  msg.BornTime,
				Body:      msg.Body,
			})
			if dlqErr != nil {
				klog.Errorf("Failed to save message to dead letter queue: %v", dlqErr)
			}
		} else {
			// Schedule async retry via delay queue
			backoff := p.cfg.RetryInterval * (1 << uint(msg.RetryCount))
			if backoff > p.cfg.RetryMaxInterval || backoff <= 0 {
				backoff = p.cfg.RetryMaxInterval
			}
			klog.V(4).Infof("Scheduling retry for message %s (attempt %d/%d) in %v",
				msg.MessageID, msg.RetryCount+1, p.cfg.RetryTimes, backoff)
			retryErr := p.factory.GetDelayManager().AddRetry(ctx, &model.RetryMessage{
				Message:           *msg,
				RetryCount:        msg.RetryCount + 1,
				OriginalGroup:     p.group,
				OriginalPartition: msg.Partition,
				Delay:             backoff,
			})
			if retryErr != nil {
				klog.Errorf("Failed to schedule retry for message %s: %v", msg.MessageID, retryErr)
				// Fallback to DLQ to prevent message loss
				p.factory.GetMessageManager().SaveMessage(ctx, &model.Message{
					MessageID: msg.MessageID,
					Topic:     msg.Topic + "_dead",
					Partition: msg.Partition,
					Key:       msg.Key,
					Tag:       msg.Tag,
					BornTime:  msg.BornTime,
					Body:      msg.Body,
				})
			}
		}

		// Advance offset (both DLQ and retry paths)
		if !isBroadcast {
			if updErr := p.updateConsumerOffset(ctx, p.group, p.topic, p.partition, p.instanceID, msg.Offset); updErr != nil {
				klog.Errorf("Failed to advance offset: %v", updErr)
			}
		} else {
			_broadcastOffset = msg.Offset + 1
		}
		continue
	}

	// Update offset tracking after successful processing
	if isBroadcast {
		_broadcastOffset = msg.Offset + 1
	} else {
		err := p.updateConsumerOffset(ctx, p.group, p.topic, p.partition, p.instanceID, msg.Offset)
		if err != nil {
			klog.Errorf("Failed to update consumer offset: %v", err)
			break
		}
	}
}
```

- [ ] **Step 3: Update callHandler signature in consume() call site**

Modify `internal/consumer/partition_consumer.go` line 91:

```go
if err := p.callHandler(msg); err != nil {
```

(Remove the `p.stopChan` parameter from the call)

- [ ] **Step 4: Commit**

```bash
git add internal/consumer/partition_consumer.go
git commit -m "feat: replace blocking retry with async delay queue retry"
```

---

### Task 9: Update partition_consumer Tests

**Files:**
- Modify: `internal/consumer/partition_consumer_test.go`

- [ ] **Step 1: Update callHandler tests for new signature**

Replace `internal/consumer/partition_consumer_test.go` line 215-236:

```go
func TestPartitionConsumer_CallHandler(t *testing.T) {
	handlerCalled := false
	handler := func(msg *model.Message) error {
		handlerCalled = true
		return nil
	}

	pc := &partitionConsumer{
		cfg:     &config.Config{},
		handler: handler,
	}

	msg := &model.Message{
		MessageID: "test-msg",
		Body:      []byte("test message"),
	}

	err := pc.callHandler(msg)
	assert.NoError(t, err)
	assert.True(t, handlerCalled)
}
```

- [ ] **Step 2: Remove obsolete retry tests**

Delete the following tests from `internal/consumer/partition_consumer_test.go`:
- `TestPartitionConsumer_CallHandler_ExponentialBackoff` (line 238-262)
- `TestPartitionConsumer_CallHandler_BackoffCapped` (line 264-291)
- `TestPartitionConsumer_CallHandler_SuccessOnRetry` (line 293-314)

These tests verified the old blocking retry behavior which no longer exists.

- [ ] **Step 3: Add test for consume() with handler failure triggering AddRetry**

Add to `internal/consumer/partition_consumer_test.go`:

```go
func TestPartitionConsumer_Consume_HandlerFailure_TriggersAddRetry(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mockFactory := new(MockFactory)
	mockMsgManager := new(MockMessageManager)
	mockConsumerManager := new(MockConsumerManager)
	mockDelayManager := new(MockDelayManager)

	mockFactory.On("GetMessageManager").Return(mockMsgManager)
	mockFactory.On("GetConsumerManager").Return(mockConsumerManager)
	mockFactory.On("GetDelayManager").Return(mockDelayManager)

	testMessages := []*model.Message{
		{
			MessageID:  "msg-1",
			Topic:      "test-topic",
			Partition:  0,
			Offset:     1,
			RetryCount: 0,
			Body:       []byte("test message"),
		},
	}

	mockConsumerManager.On("GetConsumerOffsets", mock.Anything, "test-topic", "test-group").
		Return([]model.ConsumerOffset{{Partition: 0, InstanceID: "test-instance", Offset: 0}}, nil)

	mockMsgManager.On("GetMessages", mock.Anything, "test-topic", "test-group", 0, int64(0), 100).
		Return(testMessages, nil)

	// Handler fails
	handler := func(msg *model.Message) error {
		return errors.New("handler error")
	}

	// Expect AddRetry to be called with correct parameters
	mockDelayManager.On("AddRetry", mock.Anything, mock.MatchedBy(func(msg *model.RetryMessage) bool {
		return msg.MessageID == "msg-1" &&
			msg.RetryCount == 1 &&
			msg.OriginalGroup == "test-group" &&
			msg.OriginalPartition == 0
	})).Return("msg-1", nil)

	// Expect offset to advance
	smock.ExpectExec("UPDATE mqx_consumer_offsets").
		WithArgs(int64(1), "test-group", "test-topic", 0, "test-instance").
		WillReturnResult(sqlmock.NewResult(1, 1))

	pc := &partitionConsumer{
		db:         db,
		factory:    mockFactory,
		cfg:        &config.Config{PullingInterval: time.Second, PullingSize: 100, RetryTimes: 3, RetryInterval: time.Second * 3},
		topic:      "test-topic",
		group:      "test-group",
		partition:  0,
		instanceID: "test-instance",
		handler:    handler,
		stopChan:   make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go pc.consume(ctx)
	time.Sleep(time.Millisecond * 100)
	pc.Stop(ctx)

	assert.NoError(t, smock.ExpectationsWereMet())
	mockDelayManager.AssertExpectations(t)
}
```

- [ ] **Step 4: Add MockDelayManager to test file**

Add to `internal/consumer/partition_consumer_test.go` after MockMessageManager:

```go
// MockDelayManager implements interfaces.DelayManager for testing
type MockDelayManager struct {
	mock.Mock
}

func (m *MockDelayManager) Add(ctx context.Context, msg *model.Message) (string, error) {
	args := m.Called(ctx, msg)
	return args.String(0), args.Error(1)
}

func (m *MockDelayManager) AddRetry(ctx context.Context, msg *model.RetryMessage) (string, error) {
	args := m.Called(ctx, msg)
	return args.String(0), args.Error(1)
}

func (m *MockDelayManager) DeleteMessagesByTopic(ctx context.Context, topic string) error {
	args := m.Called(ctx, topic)
	return args.Error(0)
}

func (m *MockDelayManager) Start(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockDelayManager) Stop(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}
```

- [ ] **Step 5: Add test for consume() with max retries exhausted triggering DLQ**

Add to `internal/consumer/partition_consumer_test.go`:

```go
func TestPartitionConsumer_Consume_MaxRetriesExhausted_TriggersDLQ(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mockFactory := new(MockFactory)
	mockMsgManager := new(MockMessageManager)
	mockConsumerManager := new(MockConsumerManager)
	mockDelayManager := new(MockDelayManager)

	mockFactory.On("GetMessageManager").Return(mockMsgManager)
	mockFactory.On("GetConsumerManager").Return(mockConsumerManager)
	mockFactory.On("GetDelayManager").Return(mockDelayManager)

	testMessages := []*model.Message{
		{
			MessageID:  "msg-1",
			Topic:      "test-topic",
			Partition:  0,
			Offset:     1,
			RetryCount: 2, // Already retried 2 times
			Body:       []byte("test message"),
		},
	}

	mockConsumerManager.On("GetConsumerOffsets", mock.Anything, "test-topic", "test-group").
		Return([]model.ConsumerOffset{{Partition: 0, InstanceID: "test-instance", Offset: 0}}, nil)

	mockMsgManager.On("GetMessages", mock.Anything, "test-topic", "test-group", 0, int64(0), 100).
		Return(testMessages, nil)

	// Handler fails
	handler := func(msg *model.Message) error {
		return errors.New("handler error")
	}

	// Expect SaveMessage to DLQ (not AddRetry)
	mockMsgManager.On("SaveMessage", mock.Anything, mock.MatchedBy(func(msg *model.Message) bool {
		return msg.Topic == "test-topic_dead" && msg.MessageID == "msg-1"
	})).Return("msg-1", nil)

	// Expect offset to advance
	smock.ExpectExec("UPDATE mqx_consumer_offsets").
		WithArgs(int64(1), "test-group", "test-topic", 0, "test-instance").
		WillReturnResult(sqlmock.NewResult(1, 1))

	pc := &partitionConsumer{
		db:         db,
		factory:    mockFactory,
		cfg:        &config.Config{PullingInterval: time.Second, PullingSize: 100, RetryTimes: 3},
		topic:      "test-topic",
		group:      "test-group",
		partition:  0,
		instanceID: "test-instance",
		handler:    handler,
		stopChan:   make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go pc.consume(ctx)
	time.Sleep(time.Millisecond * 100)
	pc.Stop(ctx)

	assert.NoError(t, smock.ExpectationsWereMet())
	mockMsgManager.AssertExpectations(t)
}
```

- [ ] **Step 6: Run all partition consumer tests**

Run: `cd /Users/wens/wend-dev/mqx && go test ./internal/consumer -run TestPartitionConsumer -v`

Expected: All tests PASS

- [ ] **Step 7: Commit**

```bash
git add internal/consumer/partition_consumer_test.go
git commit -m "test: update partition consumer tests for async retry"
```

---

### Task 10: Final Integration Testing

**Files:**
- None (testing only)

- [ ] **Step 1: Run all tests in the project**

Run: `cd /Users/wens/wend-dev/mqx && go test ./... -v`

Expected: All tests PASS

- [ ] **Step 2: Verify compilation**

Run: `cd /Users/wens/wend-dev/mqx && go build ./...`

Expected: Build succeeds with no errors

- [ ] **Step 3: Manual verification checklist**

Verify the following behaviors manually (if integration test environment available):

- [ ] Message handler fails once → message goes to delay queue with retry_count=1
- [ ] Delay fires → message returns to original topic+partition with retry_count=1
- [ ] Message handler fails again → message goes to delay queue with retry_count=2
- [ ] Max retries exhausted → message goes to DLQ ({topic}_dead)
- [ ] Partition consumer never blocks during retry waits
- [ ] Exponential backoff works correctly (3s, 6s, 12s, 24s, 30s cap)
- [ ] User-initiated delay messages still work (retry_count=0 path)
- [ ] Broadcast mode retries work correctly

- [ ] **Step 4: Commit final changes if any**

```bash
git add .
git commit -m "chore: final integration verification"
```

---

## Self-Review Checklist

After completing all tasks:

1. **Spec coverage verified:**
   - ✓ Schema changes (Tasks 2, 3)
   - ✓ Model updates (Task 1)
   - ✓ Interface updates (Task 4)
   - ✓ DelayManager.AddRetry implementation (Task 6)
   - ✓ MessageManager.SaveRetryMessageWithTx implementation (Task 5)
   - ✓ transferRetryMessage and processDelayMessages update (Task 7)
   - ✓ partition_consumer async retry integration (Task 8)
   - ✓ Tests (Tasks 5, 6, 9, 10)
   - ✓ Existing Add method updated for new SQL parameters (Task 6, Step 4)

2. **No placeholders found** - all steps contain complete code

3. **Type consistency verified:**
   - RetryMessage type used consistently across all tasks
   - SaveRetryMessageWithTx signature matches in interface and implementation
   - AddRetry signature matches in interface and implementation
   - RetryCount field added to Message, DelayMessage, and used in SQL consistently

4. **Self-review fixes applied:**
   - Fixed: Updated existing Add method to pass 10 parameters (was 7) after SQL template change
   - This prevents breaking user-initiated delay message functionality
