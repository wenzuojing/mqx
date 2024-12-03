package delay

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
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

func TestDelayManager_Start(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	// Expect delay messages table creation
	smock.ExpectExec("CREATE TABLE IF NOT EXISTS mqx_delay_messages").
		WillReturnResult(sqlmock.NewResult(0, 0))

	dm, err := NewDelayManager(db, new(MockFactory))
	assert.NoError(t, err)

	err = dm.Start(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, smock.ExpectationsWereMet())
}

func TestDelayManager_Add(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	dm := &delayManagerImpl{
		db:       db,
		factory:  new(MockFactory),
		interval: time.Second,
	}

	msg := &model.Message{
		Topic:    "test-topic",
		Key:      "test-key",
		Body:     []byte("test message"),
		BornTime: time.Now(),
		Delay:    time.Minute,
	}

	// Mock message insertion
	smock.ExpectExec("INSERT INTO mqx_delay_messages").
		WithArgs(
			sqlmock.AnyArg(),
			msg.Topic,
			msg.Key,
			msg.Tag,
			msg.Body,
			msg.BornTime,
			sqlmock.AnyArg(),
		).WillReturnResult(sqlmock.NewResult(1, 1))

	id, err := dm.Add(context.Background(), msg)
	assert.NoError(t, err)
	assert.NotEmpty(t, id)
	assert.NoError(t, smock.ExpectationsWereMet())
}

func TestDelayManager_ProcessDelayMessages(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mockFactory := new(MockFactory)
	mockMsgManager := new(MockMessageManager)
	mockFactory.On("GetMessageManager").Return(mockMsgManager)

	dm := &delayManagerImpl{
		db:       db,
		factory:  mockFactory,
		interval: time.Second,
		stopChan: make(chan struct{}),
	}

	// Mock lock acquisition
	smock.ExpectQuery("SELECT GET_LOCK").
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))

	// Mock delayed messages query
	now := time.Now()
	smock.ExpectQuery("SELECT message_id, topic,").
		WillReturnRows(sqlmock.NewRows([]string{
			"message_id", "topic", "key", "tag", "body", "born_time", "delay_time",
		}).AddRow(
			"msg-1", "test-topic", "test-key", "", []byte("test message"), now, now,
		))

	// Mock message transfer
	mockMsgManager.On("SaveMessage", mock.Anything, mock.AnythingOfType("*model.Message")).
		Return("msg-1", nil)

	// Mock transaction for cleanup
	smock.ExpectBegin()
	smock.ExpectExec("DELETE FROM mqx_delay_messages").
		WithArgs("msg-1").
		WillReturnResult(sqlmock.NewResult(1, 1))
	smock.ExpectCommit()

	// Mock lock release
	smock.ExpectExec("SELECT RELEASE_LOCK").
		WillReturnResult(sqlmock.NewResult(0, 0))

	// Start processing in background
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go func() {
		time.Sleep(time.Millisecond * 100)
		close(dm.stopChan)
	}()

	err = dm.processDelayMessages(ctx)
	assert.NoError(t, err)

	assert.NoError(t, smock.ExpectationsWereMet())
	mockFactory.AssertExpectations(t)
	mockMsgManager.AssertExpectations(t)
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
