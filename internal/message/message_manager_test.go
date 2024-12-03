package message

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

func TestMessageManager_SaveMessage(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mockFactory := new(MockFactory)
	mockTopicManager := new(MockTopicManager)
	mockFactory.On("GetTopicManager").Return(mockTopicManager)

	mm := &messageManagerImpl{
		db:      db,
		factory: mockFactory,
	}

	msg := &model.Message{
		Topic:    "test-topic",
		Key:      "test-key",
		Body:     []byte("test message"),
		BornTime: time.Now(),
	}

	// Mock GetTopicMeta
	mockTopicManager.On("GetTopicMeta", mock.Anything, "test-topic").Return(&model.TopicMeta{
		Topic:        "test-topic",
		PartitionNum: 3,
	}, nil)

	// Mock sequence query
	smock.ExpectBegin()
	smock.ExpectQuery("SELECT COALESCE").
		WithArgs(0).
		WillReturnRows(sqlmock.NewRows([]string{"seq"}).AddRow(1))

	// Mock message insertion
	smock.ExpectPrepare("INSERT INTO mqx_messages_test-topic")
	smock.ExpectExec("INSERT INTO mqx_messages_test-topic").
		WithArgs(
			sqlmock.AnyArg(),
			0,
			"",
			"test-key",
			[]byte("test message"),
			sqlmock.AnyArg(),
			1,
		).WillReturnResult(sqlmock.NewResult(1, 1))

	smock.ExpectCommit()

	id, err := mm.SaveMessage(context.Background(), msg)
	assert.NoError(t, err)
	assert.NotEmpty(t, id)

	assert.NoError(t, smock.ExpectationsWereMet())
	mockFactory.AssertExpectations(t)
	mockTopicManager.AssertExpectations(t)
}

func TestMessageManager_GetMessages(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mm := &messageManagerImpl{
		db: db,
	}

	now := time.Now()
	expectedMessages := []*model.Message{
		{
			MessageID: "msg-1",
			Topic:     "test-topic",
			Key:       "test-key",
			Body:      []byte("test message"),
			BornTime:  now,
			Partition: 0,
			Offset:    1,
		},
	}

	// Mock messages query
	smock.ExpectQuery("SELECT message_id, partition, tag, `key`, body, born_time, offset FROM mqx_messages_test-topic").
		WithArgs(0, int64(0), 10).
		WillReturnRows(sqlmock.NewRows([]string{
			"message_id", "partition", "tag", "key", "body", "born_time", "offset",
		}).AddRow(
			"msg-1", 0, "", "test-key", []byte("test message"), now, 1,
		))

	messages, err := mm.GetMessages(context.Background(), "test-topic", "test-group", 0, 0, 10)
	assert.NoError(t, err)
	assert.Len(t, messages, 1)
	assert.Equal(t, expectedMessages[0].MessageID, messages[0].MessageID)
	assert.Equal(t, expectedMessages[0].Key, messages[0].Key)

	assert.NoError(t, smock.ExpectationsWereMet())
}

func TestMessageManager_GetMaxOffset(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mm := &messageManagerImpl{
		db: db,
	}

	// Mock max offset query
	smock.ExpectQuery("SELECT COALESCE").
		WithArgs(0).
		WillReturnRows(sqlmock.NewRows([]string{"max_offset"}).AddRow(100))

	offset, err := mm.GetMaxOffset(context.Background(), "test-topic", 0)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), offset)

	assert.NoError(t, smock.ExpectationsWereMet())
}

func TestMessageManager_CreateMessageTable(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mm := &messageManagerImpl{
		db: db,
	}

	// Mock table creation
	smock.ExpectExec("CREATE TABLE IF NOT EXISTS mqx_messages_test-topic").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = mm.createMessageTable("test-topic", 0)
	assert.NoError(t, err)

	assert.NoError(t, smock.ExpectationsWereMet())
}

// MockTopicManager implements interfaces.TopicManager for testing
type MockTopicManager struct {
	mock.Mock
}

func (m *MockTopicManager) GetTopicMeta(ctx context.Context, topic string) (*model.TopicMeta, error) {
	args := m.Called(ctx, topic)
	return args.Get(0).(*model.TopicMeta), args.Error(1)
}

func (m *MockTopicManager) Start(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockTopicManager) Stop(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}
