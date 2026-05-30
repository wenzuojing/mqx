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

// MockTopicManager implements interfaces.TopicManager for testing
type MockTopicManager struct {
	mock.Mock
}

func (m *MockTopicManager) GetTopicMeta(ctx context.Context, topic string) (*model.TopicMeta, error) {
	args := m.Called(ctx, topic)
	return args.Get(0).(*model.TopicMeta), args.Error(1)
}

func (m *MockTopicManager) GetAllTopicMeta(ctx context.Context) ([]model.TopicMeta, error) {
	args := m.Called(ctx)
	return args.Get(0).([]model.TopicMeta), args.Error(1)
}

func (m *MockTopicManager) UpdateTopicMeta(ctx context.Context, meta *model.TopicMeta) error {
	args := m.Called(ctx, meta)
	return args.Error(0)
}

func (m *MockTopicManager) Start(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockTopicManager) Stop(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockTopicManager) CreateTopic(ctx context.Context, meta *model.TopicMeta) error {
	args := m.Called(ctx, meta)
	return args.Error(0)
}

func (m *MockTopicManager) DeleteTopic(ctx context.Context, topic string) error {
	args := m.Called(ctx, topic)
	return args.Error(0)
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

	// Mock transaction: begin -> insert message -> commit
	smock.ExpectBegin()
	smock.ExpectPrepare("INSERT INTO `mqx_messages_test-topic_0`").
		ExpectExec().
		WithArgs(
			sqlmock.AnyArg(), // message_id
			sqlmock.AnyArg(), // tag
			"test-key",
			[]byte("test message"),
			sqlmock.AnyArg(), // born_time
			sqlmock.AnyArg(), // retry_count
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

	// Mock messages query — actual SQL selects from `mqx_messages_{topic}_{partition}`
	smock.ExpectQuery("SELECT").
		WithArgs(int64(0), 10).
		WillReturnRows(sqlmock.NewRows([]string{
			"message_id", "tag", "key", "body", "born_time", "offset", "retry_count",
		}).AddRow(
			"msg-1", "", "test-key", []byte("test message"), now, 1, 0,
		))

	messages, err := mm.GetMessages(context.Background(), "test-topic", "test-group", 0, 0, 10)
	assert.NoError(t, err)
	assert.Len(t, messages, 1)
	assert.Equal(t, "msg-1", messages[0].MessageID)
	assert.Equal(t, "test-key", messages[0].Key)

	assert.NoError(t, smock.ExpectationsWereMet())
}

func TestMessageManager_GetMaxOffset(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mm := &messageManagerImpl{
		db: db,
	}

	// Mock max offset query — actual SQL: SELECT COALESCE(MAX(`offset`), 0) as max_offset FROM `%s`
	smock.ExpectQuery("SELECT COALESCE").
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
	smock.ExpectExec("CREATE TABLE IF NOT EXISTS").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = mm.createMessageTable("test-topic", 0)
	assert.NoError(t, err)

	assert.NoError(t, smock.ExpectationsWereMet())
}

func TestMessageManager_SaveMessageWithTx(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mockFactory := new(MockFactory)
	mockTopicManager := new(MockTopicManager)
	mockFactory.On("GetTopicManager").Return(mockTopicManager)

	mm := &messageManagerImpl{db: db, factory: mockFactory}

	msg := &model.Message{
		MessageID:  "retry-msg-1",
		Topic:      "test-topic",
		Key:        "key1",
		Tag:        "tag1",
		Body:       []byte("retry body"),
		BornTime:   time.Now(),
		RetryCount: 2,
	}

	mockTopicManager.On("GetTopicMeta", mock.Anything, "test-topic").Return(&model.TopicMeta{
		Topic:        "test-topic",
		PartitionNum: 3,
	}, nil)

	smock.ExpectBegin()
	smock.ExpectPrepare("INSERT INTO `mqx_messages_test-topic_0`").
		ExpectExec().
		WithArgs("retry-msg-1", "tag1", "key1", []byte("retry body"), sqlmock.AnyArg(), 2).
		WillReturnResult(sqlmock.NewResult(1, 1))

	tx, _ := db.Begin()
	err = mm.SaveMessageWithTx(context.Background(), tx, msg)
	assert.NoError(t, err)
	tx.Rollback()

	assert.NoError(t, smock.ExpectationsWereMet())
}
