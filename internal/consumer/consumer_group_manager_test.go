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

func (m *MockTopicManager) Start(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockTopicManager) Stop(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func TestConsumerGroupManager_Start(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	cgm := &consumerGroupManager{
		db:         db,
		cfg:        &config.Config{},
		group:      "test-group",
		topic:      "test-topic",
		instanceID: "test-instance",
		factory:    new(MockFactory),
		stopChan:   make(chan struct{}),
	}

	err = cgm.Start(context.Background())
	assert.NoError(t, err)

	// Stop the manager to clean up goroutines
	cgm.Stop(context.Background())
}

func TestConsumerGroupManager_Rebalance(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mockFactory := new(MockFactory)
	mockTopicManager := new(MockTopicManager)
	mockFactory.On("GetTopicManager").Return(mockTopicManager)

	// Start rebalance in background
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Mock GetTopicMeta
	mockTopicManager.On("GetTopicMeta", ctx, "test-topic").Return(&model.TopicMeta{
		Topic:        "test-topic",
		PartitionNum: 2,
	}, nil)

	cgm := &consumerGroupManager{
		db:         db,
		cfg:        &config.Config{RebalanceInterval: time.Second},
		group:      "test-group",
		topic:      "test-topic",
		instanceID: "test-instance",
		factory:    mockFactory,
		stopChan:   make(chan struct{}),
	}

	// Mock lock acquisition
	smock.ExpectQuery("SELECT GET_LOCK").
		WillReturnRows(sqlmock.NewRows([]string{"locked"}).AddRow(1))

	// Mock active instances query
	smock.ExpectQuery("SELECT group, instance_id, hostname, active, heartbeat FROM mqx_consumer_instances").
		WithArgs("test-group", "test-topic").
		WillReturnRows(sqlmock.NewRows([]string{"group", "instance_id", "hostname", "active", "heartbeat"}).
			AddRow("test-group", "test-instance", "host-1", true, time.Now()))

	// Mock partition updates
	smock.ExpectBegin()
	smock.ExpectExec("UPDATE mqx_consumer_offsets SET instance_id").
		WithArgs("test-instance", "test-group", "test-topic", 0).
		WillReturnResult(sqlmock.NewResult(1, 1))
	smock.ExpectExec("UPDATE mqx_consumer_offsets SET instance_id").
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

	// Mock heartbeat update
	smock.ExpectPrepare("INSERT INTO mqx_consumer_instances")
	smock.ExpectExec("INSERT INTO mqx_consumer_instances").
		WithArgs("test-group", "test-topic", "test-instance").
		WillReturnResult(sqlmock.NewResult(1, 1))

	success, err := cgm.updateConsumerInstanceHeartbeat(context.Background(), "test-group", "test-topic", "test-instance", "host-1")
	assert.NoError(t, err)
	assert.True(t, success)

	assert.NoError(t, smock.ExpectationsWereMet())
}

func TestConsumerGroupManager_GetActiveConsumerInstances(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	cgm := &consumerGroupManager{
		db:         db,
		group:      "test-group",
		topic:      "test-topic",
		instanceID: "test-instance",
	}

	// Mock active instances query
	smock.ExpectQuery("SELECT group, instance_id, hostname, active, heartbeat FROM mqx_consumer_instances").
		WithArgs("test-group", "test-topic", true).
		WillReturnRows(sqlmock.NewRows([]string{"group", "instance_id", "hostname", "active", "heartbeat"}).
			AddRow("test-group", "instance-1", "host-1", true, time.Now()).
			AddRow("test-group", "instance-2", "host-2", true, time.Now()))

	instances, err := cgm.getActiveConsumerInstances(context.Background(), "test-group", "test-topic")
	assert.NoError(t, err)
	assert.Len(t, instances, 2)
	assert.Equal(t, "instance-1", instances[0].InstanceID)
	assert.Equal(t, "instance-2", instances[1].InstanceID)

	assert.NoError(t, smock.ExpectationsWereMet())
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
		smock.ExpectExec("UPDATE mqx_consumer_offsets SET instance_id").
			WithArgs(p.InstanceID, p.Group, p.Topic, p.Partition).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	smock.ExpectCommit()

	err = cgm.updateConsumerPartitions(context.Background(), partitions)
	assert.NoError(t, err)

	assert.NoError(t, smock.ExpectationsWereMet())
}
