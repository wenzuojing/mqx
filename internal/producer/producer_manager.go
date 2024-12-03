package producer

import (
	"context"

	"github.com/wenzuojing/mqx/internal/interfaces"
	"github.com/wenzuojing/mqx/internal/model"
)

// ProducerManager handles message production and sending operations
func NewProducerManager(factory interfaces.Factory) (interfaces.ProducerManager, error) {
	return &producerManagerImpl{
		factory: factory,
	}, nil
}

type producerManagerImpl struct {
	factory interfaces.Factory
}

// SendSync sends a message synchronously, using delay queue if delay is set
func (p *producerManagerImpl) SendSync(ctx context.Context, msg *model.Message) (string, error) {
	if msg.Delay > 0 {
		return p.factory.GetDelayManager().Add(ctx, msg)
	}
	return p.factory.GetMessageManager().SaveMessage(ctx, msg)
}

// SendAsync sends a message asynchronously and invokes callback with the result
func (p *producerManagerImpl) SendAsync(ctx context.Context, msg *model.Message, callback func(string, error)) error {
	go func() {
		id, err := p.SendSync(ctx, msg)
		callback(id, err)
	}()
	return nil
}

func (p *producerManagerImpl) Start(ctx context.Context) error {
	return nil
}

func (p *producerManagerImpl) Stop(ctx context.Context) error {
	return nil
}
