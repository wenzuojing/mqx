package delay

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/wenzuojing/mqx/internal/interfaces"
	"github.com/wenzuojing/mqx/internal/model"
	"github.com/wenzuojing/mqx/internal/template"
	"k8s.io/klog/v2"
)

// DelayManager handles delayed message processing
func NewDelayManager(db *sql.DB, factory interfaces.Factory) (interfaces.DelayManager, error) {
	return &delayManagerImpl{db: db, factory: factory, interval: time.Second * 5}, nil
}

type delayManagerImpl struct {
	db       *sql.DB
	factory  interfaces.Factory
	interval time.Duration
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
	)
	if err != nil {
		klog.Errorf("Failed to insert delayed message: %v", err)
		return "", err
	}
	klog.V(4).Infof("Successfully added delayed message with ID: %s", msg.MessageID)
	return msg.MessageID, nil
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

func (d *delayManagerImpl) processDelayMessages(ctx context.Context) error {
	// Acquire distributed lock for delay message processing
	var lockAcquired bool
	err := d.db.QueryRow(template.GetLock, "delay_message_lock", 315360000).Scan(&lockAcquired)
	if err != nil || !lockAcquired {
		if err != nil {
			klog.Errorf("Failed to acquire delay message lock: %v", err)
		}
		time.Sleep(time.Second)
		return nil
	}

	klog.V(4).Info("Acquired delay message lock")
	// Release the lock when done
	defer func() {
		d.db.Exec(template.ReleaseLock, "delay_message_lock")
		klog.V(4).Info("Released delay message lock")
	}()

	// Process delayed messages that are ready
	tranferMessages := func() error {
		// Query messages that have reached their delay time
		rows, err := d.db.Query(template.GetReadyDelayMessages)
		if err != nil {
			klog.Errorf("Failed to query delayed messages: %v", err)
			return err
		}
		defer rows.Close()

		var messages []*model.DelayMessage
		for rows.Next() {
			var msg model.DelayMessage
			var delayTime time.Time
			err := rows.Scan(&msg.ID, &msg.MessageID, &msg.Topic, &msg.Key, &msg.Tag, &msg.Body, &msg.BornTime, &delayTime)
			if err != nil {
				klog.Warningf("Failed to scan delayed message: %v", err)
				continue
			}
			messages = append(messages, &msg)
		}

		// Process messages in a transaction
		tx, err := d.db.Begin()
		if err != nil {
			klog.Errorf("Failed to begin transaction: %v", err)
			return err
		}
		defer tx.Rollback()

		for _, msg := range messages {
			// Move message to regular message queue
			_, err := d.factory.GetMessageManager().SaveMessage(ctx, &msg.Message)
			if err != nil {
				klog.Errorf("Failed to save delayed message to store: %v", err)
				continue
			}

			// Remove processed message from delay queue
			_, err = tx.Exec(template.DeleteDelayMessage, msg.ID)
			if err != nil {
				klog.Errorf("Failed to delete processed delayed message: %v", err)
				return err
			}
		}

		if err := tx.Commit(); err != nil {
			klog.Errorf("Failed to commit delayed message transaction: %v", err)
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
			err := tranferMessages()
			if err != nil {
				klog.Errorf("Error in transfer messages cycle: %v", err)
				time.Sleep(time.Second)
				continue
			}

			elapsed := time.Since(start)
			if remaining := d.interval - elapsed; remaining > 0 {
				time.Sleep(remaining)
			}
		}
	}
}
