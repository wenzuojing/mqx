package consumer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/wenzuojing/mqx/internal/config"
	"github.com/wenzuojing/mqx/internal/interfaces"
	"github.com/wenzuojing/mqx/internal/model"
	"github.com/wenzuojing/mqx/internal/template"
	"k8s.io/klog/v2"
)

// consumerGroupManager handles consumer group rebalancing and heartbeat
type consumerGroupManager struct {
	db             *sql.DB
	cfg            *config.Config
	group          string
	topic          string
	instanceID     string
	hostname       string
	factory        interfaces.Factory
	stopChan       chan struct{}
	partitionsHash string
}

func (c *consumerGroupManager) Start(ctx context.Context) error {
	klog.V(4).Infof("Starting rebalance manager for group: %s, topic: %s", c.group, c.topic)
	c.updateConsumerInstanceHeartbeat(ctx, c.group, c.topic, c.instanceID, c.hostname)
	go func() {
		for {
			select {
			case <-c.stopChan:
				return
			default:
				c.rebalance(context.Background())
			}
		}
	}()
	go c.heartbeat(ctx)
	return nil
}

func (c *consumerGroupManager) Stop(ctx context.Context) error {
	_, err := c.db.Exec(template.UpdateConsumerInstanceUnactive, c.group, c.topic, c.instanceID)
	return err
}

// rebalance performs consumer group partition rebalancing
func (c *consumerGroupManager) rebalance(ctx context.Context) error {
	var lockAcquired bool
	err := c.db.QueryRow("SELECT GET_LOCK('rebalance_lock', 315360000)").Scan(&lockAcquired)
	if err != nil || !lockAcquired {
		if err != nil {
			klog.Errorf("Failed to acquire rebalance lock: %v", err)
		}
		time.Sleep(time.Second)
		return nil
	}

	klog.V(4).Info("Acquired rebalance lock")
	// Release the lock when we're done
	defer func() {
		c.db.Exec("SELECT RELEASE_LOCK('rebalance_lock')")
		klog.V(4).Info("Released rebalance lock")
	}()

	for {
		select {
		case <-c.stopChan:
			return nil
		default:
			start := time.Now()
			err := c.doRebalance(ctx)
			if err != nil {
				klog.Errorf("Failed to rebalance: %v", err)
			}
			elapsed := time.Since(start)
			if remaining := c.cfg.RebalanceInterval - elapsed; remaining > 0 {
				time.Sleep(remaining)
			}
		}
	}
}

func (c *consumerGroupManager) heartbeat(ctx context.Context) {
	klog.V(4).Infof("Starting heartbeat for group: %s, topic: %s", c.group, c.topic)
	heartbeatTicker := time.NewTicker(c.cfg.HeartbeatInterval)
	defer heartbeatTicker.Stop()
	for {
		select {
		case <-c.stopChan:
			return
		case <-heartbeatTicker.C:
			success, err := c.updateConsumerInstanceHeartbeat(ctx, c.group, c.topic, c.instanceID, c.hostname)
			if err != nil {
				klog.Errorf("Heartbeat failed: %v", err)
				continue
			}
			if !success {
				klog.Warning("Heartbeat was not successful")
			}
		}
	}
}

func (c *consumerGroupManager) doRebalance(ctx context.Context) error {
	klog.V(4).Infof("Starting rebalance for group: %s, topic: %s", c.group, c.topic)
	instances, err := c.getActiveConsumerInstances(ctx, c.group, c.topic)
	if err != nil {
		return errors.Wrap(err, "failed to get active consumer instances")
	}

	if len(instances) == 0 {
		klog.Errorf("No active consumer instances found for group: %s, topic: %s", c.group, c.topic)
		return nil
	}

	topicMeta, err := c.factory.GetTopicManager().GetTopicMeta(ctx, c.topic)
	if err != nil {
		return errors.Wrap(err, "failed to get topic metadata")
	}

	klog.V(4).Infof("Rebalancing %d partitions across %d instances",
		topicMeta.PartitionNum, len(instances))

	var partitions []model.ConsumerPartition
	for i := 0; i < topicMeta.PartitionNum; i++ {
		instance := instances[i%len(instances)]
		partitions = append(partitions, model.ConsumerPartition{
			Group:      c.group,
			Topic:      c.topic,
			Partition:  i,
			InstanceID: instance.InstanceID,
		})
	}

	partitionsHash := hashPartitions(partitions)
	if partitionsHash == c.partitionsHash {
		return nil
	}
	if err := c.updateConsumerPartitions(ctx, partitions); err != nil {
		return errors.Wrap(err, "failed to rebalance consumer partitions")
	}
	c.partitionsHash = partitionsHash
	klog.V(4).Info("Rebalance completed successfully")
	return nil
}

func (c *consumerGroupManager) updateConsumerInstanceHeartbeat(ctx context.Context, group string, topic string, instanceID string, hostname string) (bool, error) {
	result, err := c.db.Exec(template.UpdateConsumerInstanceHeartbeat, group, topic, instanceID)
	if err != nil {
		return false, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	if rowsAffected > 0 {
		return true, nil
	}

	_, err = c.db.Exec(template.InsertConsumerInstanceHeartbeat, group, topic, instanceID, hostname)

	if err != nil {
		return false, err
	}
	return true, nil
}

func (c *consumerGroupManager) getActiveConsumerInstances(ctx context.Context, group string, topic string) ([]model.ConsumerInstance, error) {
	rows, err := c.db.Query(template.GetActiveConsumerInstances, group, topic)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	instances := make([]model.ConsumerInstance, 0)
	for rows.Next() {
		var instance model.ConsumerInstance
		err = rows.Scan(&instance.Group, &instance.Topic, &instance.InstanceID, &instance.Hostname, &instance.Active, &instance.Heartbeat)
		if err != nil {
			return nil, err
		}
		instances = append(instances, instance)
	}
	return instances, nil
}

func (c *consumerGroupManager) updateConsumerPartitions(ctx context.Context, partitions []model.ConsumerPartition) error {
	klog.Infof("Rebalancing consumer partitions for %d partitions", len(partitions))

	tx, err := c.db.Begin()
	if err != nil {
		klog.Errorf("Failed to begin transaction for rebalance: %v", err)
		return err
	}
	defer tx.Rollback()

	for _, p := range partitions {
		klog.V(4).Infof("Assigning partition %d of topic %s to instance %s",
			p.Partition, p.Topic, p.InstanceID)
		// Update consumer offset record
		result, err := tx.Exec(template.UpdateConsumerInstanceId,
			p.InstanceID, p.Group, p.Topic, p.Partition)

		if err != nil {
			return err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			// Insert new consumer offset record
			_, err = tx.Exec(template.InsertConsumerOffset,
				p.Group, p.Topic, p.Partition, -1, p.InstanceID)
			if err != nil {
				if strings.Contains(err.Error(), "Duplicate entry") {
					continue
				}
				return err
			}
		}

	}

	if err := tx.Commit(); err != nil {
		klog.Errorf("Failed to commit rebalance transaction: %v", err)
		return err
	}
	klog.Info("Consumer partition rebalance completed successfully")
	return nil
}

func hashPartitions(partitions []model.ConsumerPartition) string {
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].Partition < partitions[j].Partition
	})
	h := sha256.New()
	for _, p := range partitions {
		h.Write([]byte(fmt.Sprintf("%d:%s:%s:%s", p.Partition, p.Group, p.Topic, p.InstanceID)))
	}
	return hex.EncodeToString(h.Sum(nil))
}
