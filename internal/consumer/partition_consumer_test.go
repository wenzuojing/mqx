package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/wenzuojing/mqx/internal/config"
	"github.com/wenzuojing/mqx/internal/interfaces"
	"github.com/wenzuojing/mqx/internal/model"
)

// MockFactory implements interfaces.Factory for testing
type MockFactory struct {
	mock.Mock
}

func (m *MockFactory) GetMessageManager() interfaces.MessageManager {
	args := m.Called()
	return args.Get(0).(interfaces.MessageManager)
}

func (m *MockFactory) GetTopicManager() interfaces.TopicManager {
	args := m.Called()
	return args.Get(0).(interfaces.TopicManager)
}

func (m *MockFactory) GetConsumerManager() interfaces.ConsumerManager {
	args := m.Called()
	return args.Get(0).(interfaces.ConsumerManager)
}

func (m *MockFactory) GetProducerManager() interfaces.ProducerManager {
	args := m.Called()
	return args.Get(0).(interfaces.ProducerManager)
}

func (m *MockFactory) GetDelayManager() interfaces.DelayManager {
	args := m.Called()
	return args.Get(0).(interfaces.DelayManager)
}

func (m *MockFactory) GetClearManager() interfaces.ClearManager {
	args := m.Called()
	return args.Get(0).(interfaces.ClearManager)
}

// MockMessageManager implements interfaces.MessageManager for testing
type MockMessageManager struct {
	mock.Mock
}

func (m *MockMessageManager) Start(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockMessageManager) Stop(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockMessageManager) SaveMessage(ctx context.Context, msg *model.Message) (string, error) {
	args := m.Called(ctx, msg)
	return args.String(0), args.Error(1)
}

func (m *MockMessageManager) GetMessages(ctx context.Context, topic string, group string, partition int, offset int64, size int) ([]*model.Message, error) {
	args := m.Called(ctx, topic, group, partition, offset, size)
	return args.Get(0).([]*model.Message), args.Error(1)
}

func (m *MockMessageManager) GetMaxOffset(ctx context.Context, topic string, partition int) (int64, error) {
	args := m.Called(ctx, topic, partition)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockMessageManager) GetPartitionStat(ctx context.Context, topic string, partition int) (*interfaces.PartitionStat, error) {
	args := m.Called(ctx, topic, partition)
	return args.Get(0).(*interfaces.PartitionStat), args.Error(1)
}

func (m *MockMessageManager) DeleteMessages(ctx context.Context, topic string, partition int) error {
	args := m.Called(ctx, topic, partition)
	return args.Error(0)
}

func (m *MockMessageManager) QueryMessageForPage(ctx context.Context, topic string, partition int, messageID string, tag string, pageNo int, pageSize int) (int, []*model.Message, error) {
	args := m.Called(ctx, topic, partition, messageID, tag, pageNo, pageSize)
	return args.Int(0), args.Get(1).([]*model.Message), args.Error(2)
}

func TestPartitionConsumer_Start(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mockFactory := new(MockFactory)
	mockConsumerManager := new(MockConsumerManager)
	mockFactory.On("GetConsumerManager").Return(mockConsumerManager)

	// Mock GetConsumerOffsets so the consume goroutine doesn't panic
	mockConsumerManager.On("GetConsumerOffsets", mock.Anything, "test-topic", "test-group").
		Maybe().Return([]model.ConsumerOffset{}, nil)

	pc := &partitionConsumer{
		db:         db,
		factory:    mockFactory,
		cfg:        &config.Config{PullingInterval: time.Second},
		topic:      "test-topic",
		group:      "test-group",
		partition:  0,
		instanceID: "test-instance",
		handler:    func(msg *model.Message) error { return nil },
		stopChan:   make(chan struct{}),
	}

	err = pc.Start(context.Background())
	assert.NoError(t, err)

	// Stop to clean up goroutine
	time.Sleep(time.Millisecond * 100)
	pc.Stop(context.Background())
}

func TestPartitionConsumer_Stop(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	pc := &partitionConsumer{
		db:       db,
		stopChan: make(chan struct{}),
	}

	err = pc.Stop(context.Background())
	assert.NoError(t, err)
}

func TestPartitionConsumer_Consume(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mockFactory := new(MockFactory)
	mockMsgManager := new(MockMessageManager)
	mockConsumerManager := new(MockConsumerManager)
	mockFactory.On("GetMessageManager").Return(mockMsgManager)
	mockFactory.On("GetConsumerManager").Return(mockConsumerManager)

	testMessages := []*model.Message{
		{
			MessageID: "msg-1",
			Topic:     "test-topic",
			Partition: 0,
			Offset:    1,
			Body:      []byte("test message 1"),
		},
	}

	// Mock getOffset via GetConsumerOffsets
	mockConsumerManager.On("GetConsumerOffsets", mock.Anything, "test-topic", "test-group").
		Return([]model.ConsumerOffset{{Partition: 0, InstanceID: "test-instance", Offset: 0}}, nil)

	mockMsgManager.On("GetMessages",
		mock.Anything,
		"test-topic",
		"test-group",
		0,
		int64(0),
		100,
	).Return(testMessages, nil)

	smock.ExpectExec("UPDATE mqx_consumer_offsets").
		WithArgs(int64(1), "test-group", "test-topic", 0, "test-instance").
		WillReturnResult(sqlmock.NewResult(1, 1))

	pc := &partitionConsumer{
		db:         db,
		factory:    mockFactory,
		cfg:        &config.Config{PullingInterval: time.Second, PullingSize: 100},
		topic:      "test-topic",
		group:      "test-group",
		partition:  0,
		instanceID: "test-instance",
		handler:    func(msg *model.Message) error { return nil },
		stopChan:   make(chan struct{}),
	}

	// Start consumer in goroutine
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go pc.consume(ctx)

	// Wait for some processing
	time.Sleep(time.Millisecond * 100)
	pc.Stop(ctx)

	assert.NoError(t, smock.ExpectationsWereMet())
	mockFactory.AssertExpectations(t)
	mockMsgManager.AssertExpectations(t)
}

func TestPartitionConsumer_CallHandler(t *testing.T) {
	handlerCalled := false
	handler := func(msg *model.Message) error {
		handlerCalled = true
		return nil
	}

	pc := &partitionConsumer{
		cfg:     &config.Config{RetryTimes: 3, RetryInterval: time.Millisecond},
		handler: handler,
	}

	msg := &model.Message{
		MessageID: "test-msg",
		Body:      []byte("test message"),
	}

	err := pc.callHandler(msg)
	assert.NoError(t, err)
	assert.True(t, handlerCalled)
}
