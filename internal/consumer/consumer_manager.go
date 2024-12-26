package consumer

import (
	"context"
	"database/sql"
	"os"
	"sync"

	"github.com/google/uuid"
	"github.com/wenzuojing/mqx/internal/config"
	"github.com/wenzuojing/mqx/internal/interfaces"
	"github.com/wenzuojing/mqx/internal/model"
	"github.com/wenzuojing/mqx/internal/template"
	"github.com/wenzuojing/mqx/pkg/templatex"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
)

// NewConsumerManager creates a new consumer manager instance
func NewConsumerManager(db *sql.DB, cfg *config.Config, factory interfaces.Factory) (interfaces.ConsumerManager, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	return &consumerManagerImpl{
		db:                        db,
		cfg:                       cfg,
		factory:                   factory,
		instanceID:                uuid.NewString(),
		hostname:                  hostname,
		consumerRebalanceManagers: make(map[string]consumerGroupManager),
	}, nil
}

type consumerManagerImpl struct {
	db                        *sql.DB
	cfg                       *config.Config
	factory                   interfaces.Factory
	consumerRebalanceManagers map[string]consumerGroupManager
	groupConsumers            []*groupConsumer
	instanceID                string
	hostname                  string
	mu                        sync.Mutex
}

func (c *consumerManagerImpl) Start(ctx context.Context) error {
	klog.Info("Starting consumer service...")
	// Check if consumer offset table exists, create if not
	if _, err := c.db.Exec(template.CreateConsumerOffsetsTable); err != nil {
		klog.Errorf("Failed to create consumer_offsets table: %v", err)
		return err
	}
	klog.V(2).Info("Created/verified consumer_offsets table")

	// Check if consumer instance table exists, create if not
	if _, err := c.db.Exec(template.CreateConsumerInstancesTable); err != nil {
		klog.Errorf("Failed to create consumer_instances table: %v", err)
		return err
	}

	klog.V(2).Info("Created/verified consumer_instances table")
	return nil
}

func (c *consumerManagerImpl) Stop(ctx context.Context) error {
	klog.Info("Stopping consumer service...")
	g := new(errgroup.Group)
	for _, manager := range c.consumerRebalanceManagers {
		m := manager
		g.Go(func() error {
			klog.V(4).Infof("Stopping rebalance manager for group: %s", m.group)
			return m.Stop(ctx)
		})
	}
	for _, gc := range c.groupConsumers {
		consumer := gc
		g.Go(func() error {
			klog.V(4).Infof("Stopping group consumer for topic: %s, group: %s",
				consumer.topic, consumer.group)
			return consumer.Stop(ctx)
		})
	}
	return g.Wait()
}

func (c *consumerManagerImpl) Consume(ctx context.Context, topic string, group string, handler func(msg *model.Message) error) error {
	klog.Infof("Setting up consumer for topic: %s, group: %s", topic, group)
	c.mu.Lock()
	defer c.mu.Unlock()
	key := group + ":" + topic
	if _, ok := c.consumerRebalanceManagers[key]; !ok {
		klog.V(4).Infof("Creating new rebalance manager for %s", key)
		manager := consumerGroupManager{
			db:         c.db,
			cfg:        c.cfg,
			group:      group,
			topic:      topic,
			factory:    c.factory,
			instanceID: c.instanceID,
			hostname:   c.hostname,
			stopChan:   make(chan struct{}),
		}
		if err := manager.Start(ctx); err != nil {
			klog.Errorf("Failed to start rebalance manager: %v", err)
			return err
		}
		c.consumerRebalanceManagers[key] = manager
	}
	gc := &groupConsumer{
		db:         c.db,
		cfg:        c.cfg,
		factory:    c.factory,
		group:      group,
		topic:      topic,
		instanceID: c.instanceID,
		handler:    handler,
	}
	if err := gc.Start(ctx); err != nil {
		klog.Errorf("Failed to start group consumer: %v", err)
		return err
	}
	c.groupConsumers = append(c.groupConsumers, gc)
	klog.Infof("Successfully set up consumer for topic: %s, group: %s", topic, group)
	return nil
}

func (c *consumerManagerImpl) GetConsumerOffsets(ctx context.Context, topic string, group string) ([]model.ConsumerOffset, error) {
	args := []any{topic}
	if group != "" {
		args = append(args, group)
	}
	sql, err := templatex.Rander(template.SelectConsumerOffsets, map[string]any{
		"Topic": topic,
		"Group": group,
	})
	if err != nil {
		return nil, err
	}
	rows, err := c.db.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var partitions []model.ConsumerOffset
	for rows.Next() {
		var partition model.ConsumerOffset
		if err := rows.Scan(&partition.Topic, &partition.Group, &partition.Partition, &partition.Offset, &partition.InstanceID); err != nil {
			return nil, err
		}
		partitions = append(partitions, partition)
	}
	return partitions, nil
}

func (c *consumerManagerImpl) GetConsumerInstances(ctx context.Context, topic string, group string) ([]model.ConsumerInstance, error) {
	args := []any{topic}
	if group != "" {
		args = append(args, group)
	}
	sql, err := templatex.Rander(template.SelectConsumerInstances, map[string]any{
		"Topic": topic,
		"Group": group,
	})

	if err != nil {
		return nil, err
	}
	rows, err := c.db.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var instances []model.ConsumerInstance
	for rows.Next() {
		var instance model.ConsumerInstance
		if err := rows.Scan(&instance.Group, &instance.Topic, &instance.InstanceID, &instance.Hostname, &instance.Active, &instance.Heartbeat); err != nil {
			return nil, err
		}
		instances = append(instances, instance)
	}
	return instances, nil
}

func (c *consumerManagerImpl) DeleteConsumerOffsets(ctx context.Context, topic string) error {
	_, err := c.db.ExecContext(ctx, template.DeleteConsumerOffsets, topic)
	if err != nil {
		return err
	}
	return nil
}
