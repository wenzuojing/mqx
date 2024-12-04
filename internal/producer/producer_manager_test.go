package producer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

func (m *MockMessageManager) Start(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockMessageManager) Stop(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

// MockDelayManager implements interfaces.DelayManager for testing
type MockDelayManager struct {
	mock.Mock
}

func (m *MockDelayManager) Add(ctx context.Context, msg *model.Message) (string, error) {
	args := m.Called(ctx, msg)
	return args.String(0), args.Error(1)
}

func (m *MockDelayManager) Start(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockDelayManager) Stop(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func TestProducerManager_SendSync_Normal(t *testing.T) {
	mockFactory := new(MockFactory)
	mockMsgManager := new(MockMessageManager)
	mockFactory.On("GetMessageManager").Return(mockMsgManager)

	pm := &producerManagerImpl{
		factory: mockFactory,
	}

	msg := &model.Message{
		Topic:    "test-topic",
		Key:      "test-key",
		Body:     []byte("test message"),
		BornTime: time.Now(),
	}

	expectedID := "msg-1"
	mockMsgManager.On("SaveMessage", mock.Anything, msg).Return(expectedID, nil)

	id, err := pm.SendSync(context.Background(), msg)
	assert.NoError(t, err)
	assert.Equal(t, expectedID, id)

	mockFactory.AssertExpectations(t)
	mockMsgManager.AssertExpectations(t)
}

func TestProducerManager_SendSync_Delayed(t *testing.T) {
	mockFactory := new(MockFactory)
	mockDelayManager := new(MockDelayManager)
	mockFactory.On("GetDelayManager").Return(mockDelayManager)

	pm := &producerManagerImpl{
		factory: mockFactory,
	}

	msg := &model.Message{
		Topic:    "test-topic",
		Key:      "test-key",
		Body:     []byte("test message"),
		BornTime: time.Now(),
		Delay:    time.Minute,
	}

	expectedID := "delayed-msg-1"
	mockDelayManager.On("Add", mock.Anything, msg).Return(expectedID, nil)

	id, err := pm.SendSync(context.Background(), msg)
	assert.NoError(t, err)
	assert.Equal(t, expectedID, id)

	mockFactory.AssertExpectations(t)
	mockDelayManager.AssertExpectations(t)
}

func TestProducerManager_SendAsync(t *testing.T) {
	mockFactory := new(MockFactory)
	mockMsgManager := new(MockMessageManager)
	mockFactory.On("GetMessageManager").Return(mockMsgManager)

	pm := &producerManagerImpl{
		factory: mockFactory,
	}

	msg := &model.Message{
		Topic:    "test-topic",
		Key:      "test-key",
		Body:     []byte("test message"),
		BornTime: time.Now(),
	}

	expectedID := "msg-1"
	mockMsgManager.On("SaveMessage", mock.Anything, msg).Return(expectedID, nil)

	callbackCalled := make(chan struct{})
	callback := func(id string, err error) {
		assert.NoError(t, err)
		assert.Equal(t, expectedID, id)
		close(callbackCalled)
	}

	err := pm.SendAsync(context.Background(), msg, callback)
	assert.NoError(t, err)

	select {
	case <-callbackCalled:
		// Success
	case <-time.After(time.Second):
		t.Fatal("Callback was not called within timeout")
	}

	mockFactory.AssertExpectations(t)
	mockMsgManager.AssertExpectations(t)
}

func TestProducerManager_Lifecycle(t *testing.T) {
	pm := &producerManagerImpl{
		factory: new(MockFactory),
	}

	err := pm.Start(context.Background())
	assert.NoError(t, err)

	err = pm.Stop(context.Background())
	assert.NoError(t, err)
}
