package consumer

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/wenzuojing/mqx/internal/config"
	"github.com/wenzuojing/mqx/internal/interfaces"
	"github.com/wenzuojing/mqx/internal/model"
	"github.com/wenzuojing/mqx/internal/template"
	"k8s.io/klog/v2"
)

// partitionConsumer handles message consumption for a specific partition
type partitionConsumer struct {
	db         *sql.DB
	factory    interfaces.Factory
	cfg        *config.Config
	topic      string
	group      string
	partition  int
	instanceID string
	handler    func(msg *model.Message) error
	stopChan   chan struct{}
}

func (p *partitionConsumer) Start(ctx context.Context) error {
	klog.V(4).Infof("Starting partition consumer for topic: %s, partition: %d", p.topic, p.partition)
	go p.consume(context.Background())
	return nil
}

func (p *partitionConsumer) Stop(ctx context.Context) error {
	klog.V(4).Infof("Stopping partition consumer for topic: %s, partition: %d", p.topic, p.partition)
	close(p.stopChan)
	return nil
}

func (p *partitionConsumer) consume(ctx context.Context) {
	_offset := int64(0)
	for {
		select {
		case <-p.stopChan:
			klog.V(4).Info("Partition consumer received stop signal")
			return
		default:
			start := time.Now()
			offset := int64(0)
			if p.group == "__broadcast__" {
				if _offset > 0 {
					offset = _offset
				} else {
					// For broadcast mode, start from the latest offset
					maxOffset, err := p.factory.GetMessageManager().GetMaxOffset(ctx, p.topic, p.partition)
					if err != nil {
						klog.Errorf("Failed to get max offset: %v", err)
						time.Sleep(time.Second * 5)
						break
					}
					offset = maxOffset

				}
			} else {

				lastOffset, err := p.getOffset(ctx, p.group, p.topic, p.partition, p.instanceID)
				if err != nil {
					klog.Errorf("Failed to get consumer offset: %v, group: %s, topic: %s, partition: %d, instanceID: %s", err, p.group, p.topic, p.partition, p.instanceID)
					time.Sleep(time.Second * 5)
					break
				}
				offset = lastOffset
				if offset == 0 {
					fmt.Println("offset is 0")
				}

			}

			// Fetch messages from the current offset
			msgs, err := p.factory.GetMessageManager().GetMessages(ctx, p.topic, p.group, p.partition, offset, p.cfg.PollingSize)
			if err != nil {
				klog.Errorf("Failed to get messages: %v", err)
				return
			}

			// Process fetched messages
			for index, msg := range msgs {
				if err := p.callHandler(msg); err != nil {
					klog.Errorf("Failed to process message: %v", err)
					// Move failed message to dead letter queue
					p.factory.GetMessageManager().SaveMessage(ctx, &model.Message{
						MessageID: msg.MessageID,
						Topic:     msg.Topic + "_dead",
						Partition: msg.Partition,
						Offset:    msg.Offset,
						Key:       msg.Key,
						BornTime:  msg.BornTime,
						Body:      msg.Body,
					})
					continue
				}

				if p.group == "__broadcast__" {
					_offset = offset + int64((index + 1))
				} else {
					// Update consumer offset after successful processing
					err := p.updateConsumerOffset(ctx, p.group, p.topic, p.partition, p.instanceID, msg.Offset)
					if err != nil {
						klog.Errorf("Failed to update consumer offset: %v", err)
						break
					}
				}
			}

			// Control polling interval
			elapsed := time.Since(start)
			if elapsed < p.cfg.PullingInterval {
				time.Sleep(p.cfg.PullingInterval - elapsed)
			}
		}
	}
}

func (p *partitionConsumer) updateConsumerOffset(ctx context.Context, group string, topic string, partition int, instanceID string, offset int64) error {
	result, err := p.db.Exec(template.UpdateConsumerOffset, offset, group, topic, partition, instanceID)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rowsAffected == 0 {
		return ErrOffsetUpdate
	}
	return nil
}

func (p *partitionConsumer) getOffset(ctx context.Context, group string, topic string, partition int, instanceID string) (int64, error) {
	var offset int64
	err := p.db.QueryRow(template.GetConsumerOffset, group, topic, partition, instanceID).Scan(&offset)
	if err == sql.ErrNoRows {
		return 0, ErrOffsetNotFound
	}
	if err != nil {
		return 0, err
	}
	return offset, nil
}

// callHandler executes message handler with retry mechanism
func (p *partitionConsumer) callHandler(msg *model.Message) error {
	for i := 0; i < p.cfg.RetryTimes; i++ {
		if err := p.handler(msg); err != nil {
			klog.Warningf("Message handling failed (attempt %d/%d): %v", i+1, p.cfg.RetryTimes, err)
			if i < p.cfg.RetryTimes-1 {
				time.Sleep(p.cfg.RetryInterval)
				continue
			}
			return err
		}
		return nil
	}
	return nil
}
