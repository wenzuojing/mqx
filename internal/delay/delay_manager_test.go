package delay

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/wenzuojing/mqx/internal/model"
)

func TestDelayManager_AddRetry(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	dm := &delayManagerImpl{db: db, stopChan: make(chan struct{})}

	retryMsg := &model.RetryMessage{
		Message: model.Message{
			MessageID: "retry-msg-1",
			Topic:     "test-topic",
			Key:       "key1",
			Tag:       "tag1",
			Body:      []byte("retry body"),
			BornTime:  time.Now(),
		},
		RetryCount: 2,
		Delay:      time.Second * 10,
	}

	mock.ExpectExec("INSERT INTO mqx_delay_messages").
		WithArgs(
			"retry-msg-1",
			"test-topic",
			"key1",
			"tag1",
			[]byte("retry body"),
			sqlmock.AnyArg(), // bornTime
			sqlmock.AnyArg(), // delayTime
			2,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	msgID, err := dm.AddRetry(context.Background(), retryMsg)
	assert.NoError(t, err)
	assert.Equal(t, "retry-msg-1", msgID)

	assert.NoError(t, mock.ExpectationsWereMet())
}
