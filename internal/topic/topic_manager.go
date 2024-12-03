package topic

import (
	"context"
	"database/sql"

	"github.com/wenzuojing/mqx/internal/interfaces"
	"github.com/wenzuojing/mqx/internal/model"
	"github.com/wenzuojing/mqx/internal/template"
	"k8s.io/klog/v2"
)

// NewTopicManager creates a new topic manager with default partition settings
func NewTopicManager(db *sql.DB, defaultPartitionNum int) (interfaces.TopicManager, error) {
	return &topicManager{db: db, defaultPartitionNum: defaultPartitionNum}, nil
}

type topicManager struct {
	db                  *sql.DB
	defaultPartitionNum int
}

func (t *topicManager) Start(ctx context.Context) error {
	if _, err := t.db.Exec(template.CreateTopicMetaTable); err != nil {
		klog.Errorf("Failed to create topic_metas table: %v", err)
		return err
	}
	klog.V(2).Info("Created/verified topic_metas table")
	return nil
}

func (t *topicManager) Stop(ctx context.Context) error {
	return nil
}

func (s *topicManager) GetTopicMeta(ctx context.Context, topic string) (*model.TopicMeta, error) {
	stmt, err := s.db.Prepare(template.GetTopicMeta)

	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var meta model.TopicMeta
	err = stmt.QueryRow(topic).Scan(&meta.Topic, &meta.PartitionNum)
	if err == sql.ErrNoRows {
		_, err = s.db.Exec(template.InsertTopicMeta, topic, s.defaultPartitionNum)
		if err != nil {
			return nil, err
		}
		return &model.TopicMeta{Topic: topic, PartitionNum: s.defaultPartitionNum}, nil
	}
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func (t *topicManager) GetAllTopicMeta(ctx context.Context) ([]model.TopicMeta, error) {
	stmt, err := t.db.Prepare(template.GetTopicMetaList)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metas []model.TopicMeta
	for rows.Next() {
		var meta model.TopicMeta
		err := rows.Scan(&meta.Topic, &meta.PartitionNum)
		if err != nil {
			return nil, err
		}
		metas = append(metas, meta)
	}
	return metas, nil
}
