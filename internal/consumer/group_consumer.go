package consumer

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/wenzuojing/mqx/internal/config"
	"github.com/wenzuojing/mqx/internal/interfaces"
	"github.com/wenzuojing/mqx/internal/model"
	"k8s.io/klog/v2"
)

// groupConsumer manages message consumption for a consumer group
type groupConsumer struct {
	db                 *sql.DB
	factory            interfaces.Factory
	cfg                *config.Config
	topic              string
	group              string
	instanceID         string
	partitionConsumers map[int]*partitionConsumer
	handler            func(msg *model.Message) error
	stopChan           chan struct{}
	mu                 sync.Mutex
}

func (g *groupConsumer) Start(ctx context.Context) error {
	klog.V(4).Infof("Starting group consumer for topic: %s, group: %s", g.topic, g.group)
	go g.consume(ctx)
	return nil
}

func (g *groupConsumer) Stop(ctx context.Context) error {
	klog.V(4).Infof("Stopping group consumer for topic: %s, group: %s", g.topic, g.group)
	close(g.stopChan)
	return nil
}

func (g *groupConsumer) consume(ctx context.Context) {
	klog.V(4).Infof("Starting consume loop for topic: %s, group: %s", g.topic, g.group)
	for {
		select {
		case <-g.stopChan:
			klog.V(4).Infof("Stopping consume loop for topic: %s, group: %s", g.topic, g.group)
			return
		default:
			start := time.Now()
			if err := g.refreshConsumerPatitions(ctx); err != nil {
				klog.Errorf("Failed to refresh consumer partitions: %v", err)
			}
			elapsed := time.Since(start)
			klog.V(4).Infof("Refresh consumer partitions took %s", elapsed)
			if elapsed < time.Second*30 {
				time.Sleep(time.Second*30 - elapsed)
			}
		}
	}
}

func (g *groupConsumer) refreshConsumerPatitions(ctx context.Context) error {
	// Get assigned partitions for this consumer instance
	consumerOffsets, err := g.getConsumerOffsets(ctx, g.group, g.topic, g.instanceID)
	if err != nil {
		klog.Errorf("Failed to get consumer partitions: %v", err)
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	// Maintain partition consumers
	for _, offset := range consumerOffsets {

		if g.partitionConsumers == nil {
			g.partitionConsumers = make(map[int]*partitionConsumer)
		}
		if _, ok := g.partitionConsumers[offset.Partition]; !ok {
			klog.V(4).Infof("Creating new partition consumer for partition %d", offset.Partition)
			pc := &partitionConsumer{
				db:         g.db,
				factory:    g.factory,
				cfg:        g.cfg,
				topic:      g.topic,
				group:      g.group,
				partition:  offset.Partition,
				instanceID: g.instanceID,
				handler:    g.handler,
				stopChan:   make(chan struct{}),
			}
			pc.Start(ctx)
			g.partitionConsumers[offset.Partition] = pc
		}

	}

	// Remove partition consumers that are no longer assigned
	for partition, pc := range g.partitionConsumers {
		exist := false
		for _, item := range consumerOffsets {
			if item.Partition == partition {
				exist = true
				break
			}
		}
		if !exist {
			klog.V(4).Infof("Removing partition consumer for partition %d", partition)
			pc.Stop(ctx)
			delete(g.partitionConsumers, partition)
		}
	}
	return nil
}

// getConsumerOffsets retrieves the partitions assigned to this consumer instance
func (g *groupConsumer) getConsumerOffsets(ctx context.Context, group, topic, instanceID string) ([]model.ConsumerOffset, error) {
	consumerOffsets, err := g.factory.GetConsumerManager().GetConsumerOffsets(ctx, topic, group)
	if err != nil {
		return nil, err
	}
	consumerOffsetsForInstance := make([]model.ConsumerOffset, 0)
	for _, offset := range consumerOffsets {
		if offset.InstanceID == instanceID {
			consumerOffsetsForInstance = append(consumerOffsetsForInstance, offset)
		}
	}
	return consumerOffsetsForInstance, nil
}
