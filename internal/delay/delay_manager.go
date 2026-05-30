package delay

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/wenzuojing/mqx/internal/config"
	"github.com/wenzuojing/mqx/internal/interfaces"
	"github.com/wenzuojing/mqx/internal/model"
	"github.com/wenzuojing/mqx/internal/template"
	"k8s.io/klog/v2"
)

// DelayManager handles delayed message processing
func NewDelayManager(db *sql.DB, cfg *config.Config, factory interfaces.Factory) (interfaces.DelayManager, error) {
	return &delayManagerImpl{db: db, factory: factory, cfg: cfg, stopChan: make(chan struct{})}, nil
}

type delayManagerImpl struct {
	db       *sql.DB
	factory  interfaces.Factory
	cfg      *config.Config
	stopChan chan struct{}
}

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
		0, // retry_count: user-initiated delays are not retries
	)
	if err != nil {
		klog.Errorf("Failed to insert delayed message: %v", err)
		return "", err
	}
	klog.V(4).Infof("Successfully added delayed message with ID: %s", msg.MessageID)
	return msg.MessageID, nil
}

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
	)
	if err != nil {
		klog.Errorf("Failed to insert retry message: %v", err)
		return "", err
	}
	klog.V(4).Infof("Successfully added retry message with ID: %s", msg.MessageID)
	return msg.MessageID, nil
}

// transferMessage moves a delay message back to its original topic queue.
// SaveMessageWithTx handles partition calculation from key and includes retry_count.
func (d *delayManagerImpl) transferMessage(ctx context.Context, tx *sql.Tx, msg *model.DelayMessage) error {
	// For retry messages, propagate the retry count to the embedded Message
	msg.Message.RetryCount = msg.RetryCount
	return d.factory.GetMessageManager().SaveMessageWithTx(ctx, tx, &msg.Message)
}

func (d *delayManagerImpl) Start(ctx context.Context) error {
	klog.Info("Starting delay manager service...")
	// Create delay message table if not exists
	if _, err := d.db.Exec(template.CreateDelayMessageTable); err != nil {
		klog.Errorf("Failed to create delay messages table: %v", err)
		return err
	}
	klog.V(2).Info("Created/verified delay messages table")

	// Start delay message processing routine
	go func() {
		for {
			select {
			case <-d.stopChan:
				return
			default:
				d.processDelayMessages(context.Background())
			}
		}
	}()
	klog.Info("Delay manager service started successfully")
	return nil
}

func (d *delayManagerImpl) Stop(ctx context.Context) error {
	klog.Info("Stopping delay manager service...")
	close(d.stopChan)
	return nil
}

func (d *delayManagerImpl) DeleteMessagesByTopic(ctx context.Context, topic string) error {
	klog.Infof("Deleting delayed messages for topic: %s", topic)
	_, err := d.db.ExecContext(ctx, template.DeleteDelayMessagesByTopic, topic)
	return err
}

func (d *delayManagerImpl) processDelayMessages(ctx context.Context) error {
	// Acquire distributed lock on a dedicated connection to ensure GET_LOCK and
	// RELEASE_LOCK operate on the same session (sql.DB is a connection pool).
	conn, err := d.db.Conn(ctx)
	if err != nil {
		klog.Errorf("Failed to get dedicated connection for delay lock: %v", err)
		time.Sleep(time.Second)
		return nil
	}

	var lockAcquired bool
	err = conn.QueryRowContext(ctx, template.GetLock, "delay_message_lock", 30).Scan(&lockAcquired)
	if err != nil || !lockAcquired {
		conn.Close()
		if err != nil {
			klog.Errorf("Failed to acquire delay message lock: %v", err)
		}
		time.Sleep(time.Second)
		return nil
	}

	klog.V(4).Info("Acquired delay message lock")
	defer func() {
		conn.ExecContext(ctx, template.ReleaseLock, "delay_message_lock")
		conn.Close()
		klog.V(4).Info("Released delay message lock")
	}()

	// Process delayed messages that are ready
	transferMessages := func() error {
		// Query messages that have reached their delay time
		rows, err := d.db.Query(template.GetReadyDelayMessages, time.Now())
		if err != nil {
			klog.Errorf("Failed to query delayed messages: %v", err)
			return err
		}
		defer rows.Close()

		var messages []*model.DelayMessage
		for rows.Next() {
			var msg model.DelayMessage
			var delayTime time.Time
			err := rows.Scan(&msg.ID, &msg.MessageID, &msg.Topic, &msg.Key, &msg.Tag, &msg.Body, &msg.BornTime, &delayTime,
				&msg.RetryCount)
			if err != nil {
				klog.Warningf("Failed to scan delayed message: %v", err)
				continue
			}
			messages = append(messages, &msg)
		}

		if len(messages) == 0 {
			return nil
		}

		// Track poison pills (messages that persistently fail to transfer)
		poisonPills := make(map[string]bool)

		// Begin transaction
		tx, err := d.db.Begin()
		if err != nil {
			klog.Errorf("Failed to begin transaction: %v", err)
			return err
		}

		for _, msg := range messages {
			if poisonPills[msg.MessageID] {
				continue
			}

			// Transfer message back to original queue (handles both user-delay and retry messages)
			err = d.transferMessage(ctx, tx, msg)

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
					key := msg.Key
					hash := 0
					for _, c := range key {
						hash = 31*hash + int(c)
					}
					partition := hash % topicMeta.PartitionNum
					if partition < 0 {
						partition = -partition
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
						klog.Errorf("Failed to begin retry transaction: %v", err)
						return err
					}
					// Re-attempt the transfer with the new table
					err = d.transferMessage(ctx, tx, msg)
				}
				if err != nil {
					// Poison pill: record failed message ID and skip it to unblock remaining messages
					klog.Errorf("Poison pill detected — message %s failed to transfer, skipping: %v", msg.MessageID, err)
					poisonPills[msg.MessageID] = true
					// Remove from delay table to prevent continuous re-processing on every cycle
					if _, delErr := d.db.Exec(template.DeleteDelayMessage, msg.ID); delErr != nil {
						klog.Errorf("Failed to delete poison pill message %d from delay table: %v", msg.ID, delErr)
					}
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

		if err := tx.Commit(); err != nil {
			klog.Errorf("Failed to commit delayed message transaction: %v", err)
			tx.Rollback()
			return err
		}
		klog.V(4).Infof("Successfully processed %d delayed messages", len(messages))
		return nil
	}

	// Main processing loop
	for {
		select {
		case <-d.stopChan:
			klog.Info("Stopping delay message processing")
			return nil
		default:
			start := time.Now()
			err := transferMessages()
			if err != nil {
				klog.Errorf("Error in transfer messages cycle: %v", err)
				time.Sleep(time.Second)
				continue
			}

			elapsed := time.Since(start)
			if remaining := d.cfg.DelayInterval - elapsed; remaining > 0 {
				time.Sleep(remaining)
			}
		}
	}
}
