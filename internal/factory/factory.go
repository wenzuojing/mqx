package factory

import (
	"database/sql"

	"github.com/wenzuojing/mqx/internal/clear"
	"github.com/wenzuojing/mqx/internal/config"
	"github.com/wenzuojing/mqx/internal/consumer"
	"github.com/wenzuojing/mqx/internal/delay"
	"github.com/wenzuojing/mqx/internal/interfaces"
	"github.com/wenzuojing/mqx/internal/message"
	"github.com/wenzuojing/mqx/internal/producer"
	"github.com/wenzuojing/mqx/internal/topic"
)

type factoryImpl struct {
	topicManager    interfaces.TopicManager
	messageManager  interfaces.MessageManager
	consumerManager interfaces.ConsumerManager
	producerManager interfaces.ProducerManager
	delayManager    interfaces.DelayManager
	clearManager    interfaces.ClearManager
}

func NewFactory(db *sql.DB, cfg *config.Config) (interfaces.Factory, error) {
	f := &factoryImpl{}

	topicManager, err := topic.NewTopicManager(db, cfg.DefaultPartitionNum)
	if err != nil {
		return nil, err
	}
	messageManager, err := message.NewMessageManager(db, f)
	if err != nil {
		return nil, err
	}
	consumerManager, err := consumer.NewConsumerManager(db, cfg, f)
	if err != nil {
		return nil, err
	}
	producerManager, err := producer.NewProducerManager(f)
	if err != nil {
		return nil, err
	}

	delayManager, err := delay.NewDelayManager(db, f)
	if err != nil {
		return nil, err
	}
	clearManager, err := clear.NewClearManger(db, cfg, f)
	if err != nil {
		return nil, err
	}
	f.topicManager = topicManager
	f.messageManager = messageManager
	f.consumerManager = consumerManager
	f.producerManager = producerManager
	f.delayManager = delayManager
	f.clearManager = clearManager
	return f, nil
}

func (f *factoryImpl) GetTopicManager() interfaces.TopicManager {
	return f.topicManager
}

func (f *factoryImpl) GetMessageManager() interfaces.MessageManager {
	return f.messageManager
}

func (f *factoryImpl) GetConsumerManager() interfaces.ConsumerManager {
	return f.consumerManager
}

func (f *factoryImpl) GetProducerManager() interfaces.ProducerManager {
	return f.producerManager
}

func (f *factoryImpl) GetDelayManager() interfaces.DelayManager {
	return f.delayManager
}

func (f *factoryImpl) GetClearManager() interfaces.ClearManager {
	return f.clearManager
}
