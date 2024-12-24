package clear

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/wenzuojing/mqx/internal/config"
	"github.com/wenzuojing/mqx/internal/interfaces"
	"github.com/wenzuojing/mqx/internal/template"
	"k8s.io/klog/v2"
)

type clearManagerImpl struct {
	db       *sql.DB
	factory  interfaces.Factory
	cfg      *config.Config
	stopChan chan struct{}
}

func NewClearManger(db *sql.DB, cfg *config.Config, factory interfaces.Factory) (interfaces.ClearManager, error) {
	return &clearManagerImpl{db: db, cfg: cfg, factory: factory, stopChan: make(chan struct{})}, nil
}

func (c *clearManagerImpl) Start(ctx context.Context) error {
	go c.clearConsumerInstance(context.Background())
	go c.clearMessage(context.Background())
	return nil
}

func (c *clearManagerImpl) Stop(ctx context.Context) error {
	close(c.stopChan)
	return nil
}

func (c *clearManagerImpl) clearConsumerInstance(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.ClearInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopChan:
			return
		case <-ticker.C:
			_, err := c.db.ExecContext(ctx, template.DeleteUnactiveConsumerInstance)
			if err != nil {
				klog.Errorf("Failed to clear consumer instances: %v", err)
			}
		}
	}
}

func (c *clearManagerImpl) clearMessage(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.ClearInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			//查询所有topicMeta
			topics, err := c.factory.GetTopicManager().GetAllTopicMeta(ctx)
			if err != nil {
				klog.Errorf("Failed to get all topic meta: %v", err)
				continue
			}

			//遍历topicMeta，根据topicMeta的partitionNum，删除对应数量的message
			for _, topic := range topics {
				partitionNum := topic.PartitionNum
				for i := 0; i < partitionNum; i++ {
					if err := c.clearMessageByPartition(ctx, topic.Topic, i, topic.RetentionDays); err != nil {
						klog.Errorf("Failed to clear message by partition, topic: %s, partition: %d, error: %v", topic.Topic, i, err)
					}
				}
			}
		}
	}
}

func (c *clearManagerImpl) clearMessageByPartition(ctx context.Context, topic string, partition int, retentionDays int) error {
	tableName := getMessageTableName(topic, partition)
	_, err := c.db.ExecContext(ctx, fmt.Sprintf(template.DeleteMessages, tableName), time.Now().Add(-time.Duration(retentionDays)*time.Hour*24))
	return err
}

func getMessageTableName(topic string, partition int) string {
	return fmt.Sprintf("mqx_messages_%s_%d", topic, partition)
}
