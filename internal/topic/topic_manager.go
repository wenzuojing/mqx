package topic

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
	"github.com/wenzuojing/mqx/internal/config"
	"github.com/wenzuojing/mqx/internal/interfaces"
	"github.com/wenzuojing/mqx/internal/model"
	"github.com/wenzuojing/mqx/internal/template"
	"k8s.io/klog/v2"
)

// NewTopicManager creates a new topic manager with default partition settings
func NewTopicManager(db *sql.DB, cfg *config.Config, factory interfaces.Factory) (interfaces.TopicManager, error) {
	return &topicManager{db: db, cfg: cfg, factory: factory}, nil
}

type topicManager struct {
	db      *sql.DB
	cfg     *config.Config
	factory interfaces.Factory
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
	err = stmt.QueryRow(topic).Scan(&meta.Topic, &meta.PartitionNum, &meta.RetentionDays)
	if err == sql.ErrNoRows {
		if err := s.CreateTopic(ctx, &model.TopicMeta{Topic: topic, PartitionNum: s.cfg.DefaultPartitionNum, RetentionDays: s.cfg.RetentionDays}); err != nil {
			return nil, err
		}
		return &model.TopicMeta{Topic: topic, PartitionNum: s.cfg.DefaultPartitionNum, RetentionDays: s.cfg.RetentionDays}, nil
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
		err := rows.Scan(&meta.Topic, &meta.PartitionNum, &meta.RetentionDays)
		if err != nil {
			return nil, err
		}
		metas = append(metas, meta)
	}
	return metas, nil
}

func (t *topicManager) UpdateTopicMeta(ctx context.Context, meta *model.TopicMeta) error {
	result, err := t.db.ExecContext(ctx, template.UpdateTopicMeta, meta.PartitionNum, meta.RetentionDays, meta.Topic)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return sql.ErrNoRows
	}

	return nil
}

func (t *topicManager) CreateTopic(ctx context.Context, meta *model.TopicMeta) error {
	_, err := t.db.ExecContext(ctx, template.InsertTopicMeta, meta.Topic, meta.PartitionNum, meta.RetentionDays)
	if err != nil {
		return err
	}
	return nil
}

func (t *topicManager) DeleteTopic(ctx context.Context, topic string) error {

	instances, err := t.factory.GetConsumerManager().GetConsumerInstances(ctx, topic)
	if err != nil {
		return errors.Wrap(err, "failed to get consumer instances")
	}

	for _, instance := range instances {
		if instance.Active {
			return fmt.Errorf("topic %s has active consumers", topic)
		}
	}

	topicMeta, err := t.GetTopicMeta(ctx, topic)
	if err != nil {
		return errors.Wrap(err, "failed to get topic meta")
	}

	//delete messages
	for i := 0; i < topicMeta.PartitionNum; i++ {
		t.factory.GetMessageManager().DeleteMessages(ctx, topicMeta.Topic, i)
	}
	//delete consumer partitions
	t.factory.GetConsumerManager().DeleteConsumerPartitions(ctx, topicMeta.Topic)

	_, err = t.db.ExecContext(ctx, template.DeleteTopicMeta, topic)
	if err != nil {
		return errors.Wrap(err, "failed to delete topic meta")
	}
	return nil
}
