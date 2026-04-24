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

func TestGroupConsumer_Start(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mockFactory := new(MockFactory)
	mockConsumerManager := new(MockConsumerManager)
	mockFactory.On("GetConsumerManager").Return(mockConsumerManager)
	mockConsumerManager.On("GetConsumerOffsets", mock.Anything, "test-topic", "test-group").
		Return([]model.ConsumerOffset{}, nil)

	gc := &groupConsumer{
		db:                 db,
		factory:            mockFactory,
		cfg:                &config.Config{RefreshConsumerPartitionsInterval: time.Second * 30},
		topic:              "test-topic",
		group:              "test-group",
		instanceID:         "test-instance",
		partitionConsumers: make(map[int]*partitionConsumer),
		handler:            func(msg *model.Message) error { return nil },
		stopChan:           make(chan struct{}),
	}

	err = gc.Start(context.Background())
	assert.NoError(t, err)

	time.Sleep(time.Millisecond * 100)
	gc.Stop(context.Background())
}

func TestGroupConsumer_Stop(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	gc := &groupConsumer{
		db:       db,
		stopChan: make(chan struct{}),
	}

	err = gc.Stop(context.Background())
	assert.NoError(t, err)
}

func TestGroupConsumer_Consume(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mockFactory := new(MockFactory)
	mockConsumerManager := new(MockConsumerManager)
	mockFactory.On("GetConsumerManager").Return(mockConsumerManager)
	mockConsumerManager.On("GetConsumerOffsets", mock.Anything, "test-topic", "test-group").
		Return([]model.ConsumerOffset{}, nil)

	gc := &groupConsumer{
		db:                 db,
		factory:            mockFactory,
		cfg:                &config.Config{RefreshConsumerPartitionsInterval: time.Second * 30},
		topic:              "test-topic",
		group:              "test-group",
		instanceID:         "test-instance",
		partitionConsumers: make(map[int]*partitionConsumer),
		handler:            func(msg *model.Message) error { return nil },
		stopChan:           make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go func() {
		time.Sleep(time.Millisecond * 200)
		gc.Stop(ctx)
	}()

	gc.consume(ctx)

	mockFactory.AssertExpectations(t)
	mockConsumerManager.AssertExpectations(t)
}

func TestGroupConsumer_GetConsumerPartitions(t *testing.T) {
	mockFactory := new(MockFactory)
	mockConsumerManager := new(MockConsumerManager)
	mockFactory.On("GetConsumerManager").Return(mockConsumerManager)

	gc := &groupConsumer{
		factory:    mockFactory,
		topic:      "test-topic",
		group:      "test-group",
		instanceID: "test-instance",
	}

	mockConsumerManager.On("GetConsumerOffsets", mock.Anything, "test-topic", "test-group").
		Return([]model.ConsumerOffset{
			{Group: "test-group", Topic: "test-topic", Partition: 0, InstanceID: "test-instance", Offset: 0},
			{Group: "test-group", Topic: "test-topic", Partition: 1, InstanceID: "test-instance", Offset: 0},
		}, nil)

	partitions, err := gc.getConsumerOffsets(context.Background(), "test-group", "test-topic", "test-instance")
	assert.NoError(t, err)
	assert.Len(t, partitions, 2)

	mockFactory.AssertExpectations(t)
	mockConsumerManager.AssertExpectations(t)
}

func TestGroupConsumer_PartitionManagement(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mockFactory := new(MockFactory)
	mockConsumerManager := new(MockConsumerManager)
	mockFactory.On("GetConsumerManager").Return(mockConsumerManager)

	gc := &groupConsumer{
		db:                 db,
		factory:            mockFactory,
		cfg:                &config.Config{PullingSize: 100, PullingInterval: time.Second},
		topic:              "test-topic",
		group:              "test-group",
		instanceID:         "test-instance",
		partitionConsumers: make(map[int]*partitionConsumer),
		handler:            func(msg *model.Message) error { return nil },
		stopChan:           make(chan struct{}),
	}

	mockConsumerManager.On("GetConsumerOffsets", mock.Anything, "test-topic", "test-group").
		Return([]model.ConsumerOffset{
			{Group: "test-group", Topic: "test-topic", Partition: 0, InstanceID: "test-instance", Offset: 0},
		}, nil)

	ctx := context.Background()
	partitions, err := gc.getConsumerOffsets(ctx, gc.group, gc.topic, gc.instanceID)
	assert.NoError(t, err)

	gc.mu.Lock()
	for _, partition := range partitions {
		assert.NotContains(t, gc.partitionConsumers, partition.Partition)
		pc := &partitionConsumer{
			db:         gc.db,
			factory:    gc.factory,
			cfg:        gc.cfg,
			topic:      gc.topic,
			group:      gc.group,
			partition:  partition.Partition,
			instanceID: gc.instanceID,
			handler:    gc.handler,
			stopChan:   make(chan struct{}),
		}
		gc.partitionConsumers[partition.Partition] = pc
	}
	gc.mu.Unlock()

	gc.mu.Lock()
	assert.Len(t, gc.partitionConsumers, 1)
	assert.Contains(t, gc.partitionConsumers, 0)
	gc.mu.Unlock()

	mockFactory.AssertExpectations(t)
	mockConsumerManager.AssertExpectations(t)
}
