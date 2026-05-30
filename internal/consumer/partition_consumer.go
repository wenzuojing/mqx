package consumer

import (
	"context"
	"database/sql"
	"strings"
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
	isBroadcast := strings.HasPrefix(p.group, "__broadcast__")
	_broadcastOffset := int64(0)

	// For broadcast mode, start from the latest offset to only consume new messages.
	// Retry until successful to avoid consuming all historical messages (offset 0).
	if isBroadcast {
	initBroadcast:
		for {
			select {
			case <-p.stopChan:
				return
			default:
				maxOffset, err := p.factory.GetMessageManager().GetMaxOffset(ctx, p.topic, p.partition)
				if err != nil {
					klog.Warningf("Failed to get max offset for broadcast, retrying: %v", err)
					time.Sleep(time.Second)
				} else {
					_broadcastOffset = maxOffset
					break initBroadcast
				}
			}
		}
	}

	for {
		select {
		case <-p.stopChan:
			klog.V(4).Info("Partition consumer received stop signal")
			return
		default:
			start := time.Now()
			var offset int64

			if isBroadcast {
				offset = _broadcastOffset
			} else {
				lastOffset, err := p.getOffset(ctx, p.group, p.topic, p.partition, p.instanceID)
				if err != nil {
					if err != ErrOffsetNotFound {
						klog.Errorf("Failed to get consumer offset: %v, group: %s, topic: %s, partition: %d, instanceID: %s", err, p.group, p.topic, p.partition, p.instanceID)
					}
					time.Sleep(time.Second * 5)
					break
				}
				offset = lastOffset
			}

			// Fetch messages from the current offset
			msgs, err := p.factory.GetMessageManager().GetMessages(ctx, p.topic, p.group, p.partition, offset, p.cfg.PullingSize)
			if err != nil {
				if !strings.Contains(err.Error(), "doesn't exist") {
					klog.Errorf("Failed to get messages: %v", err)
				}
			} else {
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
							if backoff <= 0 {
								// Overflow protection
								backoff = time.Minute * 5
							}
							klog.V(4).Infof("Scheduling retry for message %s (attempt %d/%d) in %v",
								msg.MessageID, msg.RetryCount+1, p.cfg.RetryTimes, backoff)
							_, retryErr := p.factory.GetDelayManager().AddRetry(ctx, &model.RetryMessage{
								Message:    *msg,
								RetryCount: msg.RetryCount + 1,
								Delay:      backoff,
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
	offsets, err := p.factory.GetConsumerManager().GetConsumerOffsets(ctx, topic, group)
	if err != nil {
		return 0, err
	}
	for _, offset := range offsets {
		if offset.Partition == partition && offset.InstanceID == instanceID {
			return offset.Offset, nil
		}
	}
	return 0, ErrOffsetNotFound
}

// callHandler executes message handler once without retry.
// Retry logic is now handled asynchronously via the delay queue.
func (p *partitionConsumer) callHandler(msg *model.Message) error {
	return p.handler(msg)
}
