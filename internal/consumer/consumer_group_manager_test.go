package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/wenzuojing/mqx/internal/config"
	"github.com/wenzuojing/mqx/internal/model"
)

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

// MockConsumerManager implements interfaces.ConsumerManager for testing
type MockConsumerManager struct {
	mock.Mock
}

func (m *MockConsumerManager) DeleteConsumerOffsets(ctx context.Context, topic string) error {
	args := m.Called(ctx, topic)
	return args.Error(0)
}

func (m *MockConsumerManager) GetConsumerOffsets(ctx context.Context, topic string, group string) ([]model.ConsumerOffset, error) {
	args := m.Called(ctx, topic, group)
	return args.Get(0).([]model.ConsumerOffset), args.Error(1)
}

func (m *MockConsumerManager) GetConsumerInstances(ctx context.Context, topic string, group string) ([]model.ConsumerInstance, error) {
	args := m.Called(ctx, topic, group)
	return args.Get(0).([]model.ConsumerInstance), args.Error(1)
}

func (m *MockConsumerManager) Consume(ctx context.Context, topic string, group string, handler func(msg *model.Message) error) error {
	args := m.Called(ctx, topic, group, handler)
	return args.Error(0)
}

func (m *MockConsumerManager) Start(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockConsumerManager) Stop(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockConsumerManager) GetActiveConsumerInstances(ctx context.Context, topic string, group string, heartbeatTimeoutSeconds int) ([]model.ConsumerInstance, error) {
	args := m.Called(ctx, topic, group, heartbeatTimeoutSeconds)
	return args.Get(0).([]model.ConsumerInstance), args.Error(1)
}

func TestConsumerGroupManager_Start(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mockFactory := new(MockFactory)
	mockTopicManager := new(MockTopicManager)
	mockConsumerManager := new(MockConsumerManager)
	mockFactory.On("GetTopicManager").Return(mockTopicManager)
	mockFactory.On("GetConsumerManager").Return(mockConsumerManager)

	cgm := &consumerGroupManager{
		db:         db,
		cfg:        &config.Config{HeartbeatInterval: time.Second * 30, RebalanceInterval: time.Second * 30},
		group:      "test-group",
		topic:      "test-topic",
		instanceID: "test-instance",
		factory:    mockFactory,
		stopChan:   make(chan struct{}),
	}

	// Mock initial heartbeat
	smock.ExpectExec("UPDATE mqx_consumer_instances").
		WithArgs("test-group", "test-topic", "test-instance").
		WillReturnResult(sqlmock.NewResult(0, 0))
	smock.ExpectExec("INSERT INTO mqx_consumer_instances").
		WithArgs("test-group", "test-topic", "test-instance", "").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = cgm.Start(context.Background())
	assert.NoError(t, err)

	// Stop the manager to clean up goroutines
	smock.ExpectExec("UPDATE mqx_consumer_instances").
		WithArgs("test-group", "test-topic", "test-instance").
		WillReturnResult(sqlmock.NewResult(1, 1))
	cgm.Stop(context.Background())

	// Wait for background goroutines to exit
	time.Sleep(time.Millisecond * 1100)
}

func TestConsumerGroupManager_Rebalance(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mockFactory := new(MockFactory)
	mockTopicManager := new(MockTopicManager)
	mockConsumerManager := new(MockConsumerManager)
	mockFactory.On("GetTopicManager").Return(mockTopicManager)
	mockFactory.On("GetConsumerManager").Return(mockConsumerManager)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Mock GetTopicMeta
	mockTopicManager.On("GetTopicMeta", ctx, "test-topic").Return(&model.TopicMeta{
		Topic:        "test-topic",
		PartitionNum: 2,
	}, nil)

	// Mock GetActiveConsumerInstances
	mockConsumerManager.On("GetActiveConsumerInstances", mock.Anything, "test-topic", "test-group", 90).
		Return([]model.ConsumerInstance{
			{Group: "test-group", Topic: "test-topic", InstanceID: "test-instance", Hostname: "host-1", Active: true, Heartbeat: time.Now()},
		}, nil)

	cgm := &consumerGroupManager{
		db:         db,
		cfg:        &config.Config{RebalanceInterval: time.Second, HeartbeatInterval: time.Second * 30},
		group:      "test-group",
		topic:      "test-topic",
		instanceID: "test-instance",
		factory:    mockFactory,
		stopChan:   make(chan struct{}),
	}

	// Mock lock acquisition
	smock.ExpectQuery("SELECT GET_LOCK").
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))

	// Mock partition updates
	smock.ExpectBegin()
	smock.ExpectExec("UPDATE mqx_consumer_offsets").
		WithArgs("test-instance", "test-group", "test-topic", 0).
		WillReturnResult(sqlmock.NewResult(1, 1))
	smock.ExpectExec("UPDATE mqx_consumer_offsets").
		WithArgs("test-instance", "test-group", "test-topic", 1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	smock.ExpectCommit()

	// Mock lock release
	smock.ExpectExec("SELECT RELEASE_LOCK").
		WillReturnResult(sqlmock.NewResult(0, 0))

	go func() {
		time.Sleep(time.Millisecond * 100)
		close(cgm.stopChan)
	}()

	err = cgm.rebalance(ctx)
	assert.NoError(t, err)

	assert.NoError(t, smock.ExpectationsWereMet())
	mockFactory.AssertExpectations(t)
	mockTopicManager.AssertExpectations(t)
	mockConsumerManager.AssertExpectations(t)
}

func TestConsumerGroupManager_Heartbeat(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	cgm := &consumerGroupManager{
		db:         db,
		cfg:        &config.Config{HeartbeatInterval: time.Second},
		group:      "test-group",
		topic:      "test-topic",
		instanceID: "test-instance",
		stopChan:   make(chan struct{}),
	}

	// Mock heartbeat: first UPDATE returns 0 rows, then INSERT
	smock.ExpectExec("UPDATE mqx_consumer_instances").
		WithArgs("test-group", "test-topic", "test-instance").
		WillReturnResult(sqlmock.NewResult(0, 0))
	smock.ExpectExec("INSERT INTO mqx_consumer_instances").
		WithArgs("test-group", "test-topic", "test-instance", "host-1").
		WillReturnResult(sqlmock.NewResult(1, 1))

	success, err := cgm.updateConsumerInstanceHeartbeat(context.Background(), "test-group", "test-topic", "test-instance", "host-1")
	assert.NoError(t, err)
	assert.True(t, success)

	assert.NoError(t, smock.ExpectationsWereMet())
}

func TestConsumerGroupManager_GetActiveConsumerInstances(t *testing.T) {
	mockFactory := new(MockFactory)
	mockConsumerManager := new(MockConsumerManager)
	mockFactory.On("GetConsumerManager").Return(mockConsumerManager)

	cgm := &consumerGroupManager{
		factory:    mockFactory,
		cfg:        &config.Config{HeartbeatInterval: time.Second * 30},
		group:      "test-group",
		topic:      "test-topic",
		instanceID: "test-instance",
	}

	mockConsumerManager.On("GetActiveConsumerInstances", mock.Anything, "test-topic", "test-group", 90).
		Return([]model.ConsumerInstance{
			{Group: "test-group", Topic: "test-topic", InstanceID: "instance-1", Hostname: "host-1", Active: true, Heartbeat: time.Now()},
			{Group: "test-group", Topic: "test-topic", InstanceID: "instance-2", Hostname: "host-2", Active: true, Heartbeat: time.Now()},
		}, nil)

	instances, err := cgm.getActiveConsumerInstances(context.Background(), "test-group", "test-topic")
	assert.NoError(t, err)
	assert.Len(t, instances, 2)
	assert.Equal(t, "instance-1", instances[0].InstanceID)
	assert.Equal(t, "instance-2", instances[1].InstanceID)

	mockFactory.AssertExpectations(t)
	mockConsumerManager.AssertExpectations(t)
}

func TestConsumerGroupManager_UpdateConsumerPartitions(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	cgm := &consumerGroupManager{
		db: db,
	}

	partitions := []model.ConsumerOffset{
		{
			Group:      "test-group",
			Topic:      "test-topic",
			Partition:  0,
			InstanceID: "test-instance",
		},
		{
			Group:      "test-group",
			Topic:      "test-topic",
			Partition:  1,
			InstanceID: "test-instance",
		},
	}

	// Mock transaction
	smock.ExpectBegin()
	for _, p := range partitions {
		smock.ExpectExec("UPDATE mqx_consumer_offsets").
			WithArgs(p.InstanceID, p.Group, p.Topic, p.Partition).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	smock.ExpectCommit()

	err = cgm.updateConsumerPartitions(context.Background(), partitions)
	assert.NoError(t, err)

	assert.NoError(t, smock.ExpectationsWereMet())
}
