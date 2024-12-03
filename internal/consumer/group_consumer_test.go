package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/wenzuojing/mqx/internal/config"
	"github.com/wenzuojing/mqx/internal/model"
)

func TestGroupConsumer_Start(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	gc := &groupConsumer{
		db:                 db,
		factory:            new(MockFactory),
		cfg:                &config.Config{},
		topic:              "test-topic",
		group:              "test-group",
		instanceID:         "test-instance",
		partitionConsumers: make(map[int]*partitionConsumer),
		handler:            func(msg *model.Message) error { return nil },
		stopChan:           make(chan struct{}),
	}

	err = gc.Start(context.Background())
	assert.NoError(t, err)
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
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mockFactory := new(MockFactory)
	mockMsgManager := new(MockMessageManager)
	mockFactory.On("GetMessageManager").Return(mockMsgManager)
	//mock GetMessages

	// Setup SQL mock expectations
	smock.ExpectQuery("SELECT partition FROM mqx_consumer_offsets").
		WithArgs("test-group", "test-topic", "test-instance").
		WillReturnRows(sqlmock.NewRows([]string{"partition"}).
			AddRow(0).
			AddRow(1))

	// Mock getOffset queries for both partitions
	smock.ExpectQuery("SELECT offset FROM mqx_consumer_offsets").
		WithArgs("test-group", "test-topic", 0, "test-instance").
		WillReturnRows(sqlmock.NewRows([]string{"offset"}).AddRow(0))

	smock.ExpectQuery("SELECT offset FROM mqx_consumer_offsets").
		WithArgs("test-group", "test-topic", 1, "test-instance").
		WillReturnRows(sqlmock.NewRows([]string{"offset"}).AddRow(0))

	gc := &groupConsumer{
		db:                 db,
		factory:            mockFactory,
		cfg:                &config.Config{},
		topic:              "test-topic",
		group:              "test-group",
		instanceID:         "test-instance",
		partitionConsumers: make(map[int]*partitionConsumer),
		handler:            func(msg *model.Message) error { return nil },
		stopChan:           make(chan struct{}),
	}

	// Start consumer in goroutine
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go func() {
		time.Sleep(time.Second * 10)
		gc.Stop(ctx)
	}()

	mockMsgManager.On("GetMessages", ctx, "test-topic", "test-group", 0, 0, 0).Return(nil, nil)
	mockMsgManager.On("GetMessages", ctx, "test-topic", "test-group", 1, 0, 0).Return(nil, nil)

	gc.consume(ctx)

	// Wait for partition consumers to be created

	// Verify expectations
	assert.NoError(t, smock.ExpectationsWereMet())
	mockFactory.AssertExpectations(t)

	// Verify partition consumers were created
	gc.mu.Lock()
	assert.Len(t, gc.partitionConsumers, 2)
	gc.mu.Unlock()
}

func TestGroupConsumer_GetConsumerPartitions(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	// Setup SQL mock expectations
	smock.ExpectQuery("SELECT partition FROM mqx_consumer_offsets").
		WithArgs("test-group", "test-topic", "test-instance").
		WillReturnRows(sqlmock.NewRows([]string{"partition"}).
			AddRow(0).
			AddRow(1))

	gc := &groupConsumer{
		db:         db,
		topic:      "test-topic",
		group:      "test-group",
		instanceID: "test-instance",
	}

	partitions, err := gc.getConsumerPartitions(context.Background(), "test-group", "test-topic", "test-instance")
	assert.NoError(t, err)
	assert.Len(t, partitions, 2)
	assert.Contains(t, partitions, 0)
	assert.Contains(t, partitions, 1)

	assert.NoError(t, smock.ExpectationsWereMet())
}

func TestGroupConsumer_PartitionManagement(t *testing.T) {
	db, smock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	gc := &groupConsumer{
		db:                 db,
		factory:            new(MockFactory),
		cfg:                &config.Config{},
		topic:              "test-topic",
		group:              "test-group",
		instanceID:         "test-instance",
		partitionConsumers: make(map[int]*partitionConsumer),
		handler:            func(msg *model.Message) error { return nil },
		stopChan:           make(chan struct{}),
	}

	// Test adding new partition consumer
	smock.ExpectQuery("SELECT partition FROM mqx_consumer_offsets").
		WithArgs("test-group", "test-topic", "test-instance").
		WillReturnRows(sqlmock.NewRows([]string{"partition"}).AddRow(0))

	ctx := context.Background()
	partitions, err := gc.getConsumerPartitions(ctx, gc.group, gc.topic, gc.instanceID)
	assert.NoError(t, err)

	gc.mu.Lock()
	for _, partition := range partitions {
		assert.NotContains(t, gc.partitionConsumers, partition.Partition)
		pc := &partitionConsumer{
			factory:    gc.factory,
			cfg:        gc.cfg,
			topic:      gc.topic,
			group:      gc.group,
			partition:  partition.Partition,
			instanceID: gc.instanceID,
			handler:    gc.handler,
		}
		gc.partitionConsumers[partition.Partition] = pc
	}
	gc.mu.Unlock()

	// Verify partition consumer was added
	gc.mu.Lock()
	assert.Len(t, gc.partitionConsumers, 1)
	assert.Contains(t, gc.partitionConsumers, 0)
	gc.mu.Unlock()

	assert.NoError(t, smock.ExpectationsWereMet())
}
