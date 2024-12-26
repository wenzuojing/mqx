package internal

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/wenzuojing/mqx/internal/config"
	"github.com/wenzuojing/mqx/internal/console"
	"github.com/wenzuojing/mqx/internal/factory"
	"github.com/wenzuojing/mqx/internal/interfaces"
	"github.com/wenzuojing/mqx/internal/model"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
)

type MessageHandler func(msg *model.Message) error

type MessageService interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	SendSync(ctx context.Context, msg *model.Message) (string, error)
	SendAsync(ctx context.Context, msg *model.Message, callback func(string, error)) error
	GroupSubscribe(ctx context.Context, topic string, group string, handler MessageHandler) error
	BroadcastSubscribe(ctx context.Context, topic string, handler MessageHandler) error
}

func NewMessageService(cfg *config.Config) (MessageService, error) {
	klog.V(4).Info("Creating new message service")
	db, err := sql.Open("mysql", cfg.DSN)
	// 设置最大连接数
	db.SetMaxOpenConns(100)
	// 设置最大空闲连接数
	db.SetMaxIdleConns(50)
	// 设置连接的最大存活时间
	db.SetConnMaxLifetime(time.Hour)
	// Enable automatic reconnection
	db.SetConnMaxIdleTime(time.Hour)
	// Ping database to verify connection
	if err := db.Ping(); err != nil {
		klog.Errorf("Failed to ping database: %v", err)
		return nil, err
	}
	if err != nil {
		klog.Errorf("Failed to open database connection: %v", err)
		return nil, err
	}

	factory, err := factory.NewFactory(db, cfg)
	if err != nil {
		klog.Errorf("Failed to create factory: %v", err)
		return nil, err
	}

	consoleServer := console.NewConsoleServer(cfg, factory)

	klog.V(4).Info("Message service created successfully")
	return &messageServiceImpl{
		topicManager:    factory.GetTopicManager(),
		messageManager:  factory.GetMessageManager(),
		consumerManager: factory.GetConsumerManager(),
		producerManager: factory.GetProducerManager(),
		delayManager:    factory.GetDelayManager(),
		clearManager:    factory.GetClearManager(),
		db:              db,
		consoleServer:   consoleServer,
		cfg:             cfg,
	}, nil
}

type messageServiceImpl struct {
	topicManager    interfaces.TopicManager
	messageManager  interfaces.MessageManager
	consumerManager interfaces.ConsumerManager
	producerManager interfaces.ProducerManager
	delayManager    interfaces.DelayManager
	clearManager    interfaces.ClearManager
	db              *sql.DB
	consoleServer   *console.ConsoleServer
	cfg             *config.Config
}

func (s *messageServiceImpl) Start(ctx context.Context) error {
	klog.Info("Starting message service components...")
	g := new(errgroup.Group)

	g.Go(func() error {
		klog.V(4).Info("Starting topic manager...")
		if err := s.topicManager.Start(ctx); err != nil {
			klog.Errorf("Failed to start topic manager: %v", err)
			return err
		}
		return nil
	})

	g.Go(func() error {
		klog.V(4).Info("Starting message manager...")
		if err := s.messageManager.Start(ctx); err != nil {
			klog.Errorf("Failed to start message manager: %v", err)
			return err
		}
		return nil
	})

	g.Go(func() error {
		klog.V(4).Info("Starting consumer manager...")
		if err := s.consumerManager.Start(ctx); err != nil {
			klog.Errorf("Failed to start consumer manager: %v", err)
			return err
		}
		return nil
	})

	g.Go(func() error {
		klog.V(4).Info("Starting producer manager...")
		if err := s.producerManager.Start(ctx); err != nil {
			klog.Errorf("Failed to start producer manager: %v", err)
			return err
		}
		return nil
	})

	g.Go(func() error {
		klog.V(4).Info("Starting delay manager...")
		if err := s.delayManager.Start(ctx); err != nil {
			klog.Errorf("Failed to start delay manager: %v", err)
			return err
		}
		return nil
	})

	g.Go(func() error {
		klog.V(4).Info("Starting clear manager...")
		if err := s.clearManager.Start(ctx); err != nil {
			klog.Errorf("Failed to start clear manager: %v", err)
			return err
		}
		return nil
	})

	if s.cfg.EnableConsole {
		g.Go(func() error {
			klog.V(4).Info("Starting console server...")
			if err := s.consoleServer.Start(ctx); err != nil {
				klog.Errorf("Failed to start console server: %v", err)
				return err
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		klog.Errorf("Failed to start message service: %v", err)
		return err
	}

	klog.Info("All message service components started successfully")
	return nil
}

func (s *messageServiceImpl) Stop(ctx context.Context) error {
	klog.Info("Stopping message service components...")
	g := new(errgroup.Group)

	g.Go(func() error {
		klog.V(4).Info("Stopping topic manager...")
		if err := s.topicManager.Stop(ctx); err != nil {
			klog.Errorf("Failed to stop topic manager: %v", err)
			return err
		}
		return nil
	})

	g.Go(func() error {
		klog.V(4).Info("Stopping message manager...")
		if err := s.messageManager.Stop(ctx); err != nil {
			klog.Errorf("Failed to stop message manager: %v", err)
			return err
		}
		return nil
	})

	g.Go(func() error {
		klog.V(4).Info("Stopping consumer manager...")
		if err := s.consumerManager.Stop(ctx); err != nil {
			klog.Errorf("Failed to stop consumer manager: %v", err)
			return err
		}
		return nil
	})

	g.Go(func() error {
		klog.V(4).Info("Stopping producer manager...")
		if err := s.producerManager.Stop(ctx); err != nil {
			klog.Errorf("Failed to stop producer manager: %v", err)
			return err
		}
		return nil
	})

	g.Go(func() error {
		klog.V(4).Info("Stopping delay manager...")
		if err := s.delayManager.Stop(ctx); err != nil {
			klog.Errorf("Failed to stop delay manager: %v", err)
			return err
		}
		return nil
	})

	g.Go(func() error {
		klog.V(4).Info("Stopping clear manager...")
		if err := s.clearManager.Stop(ctx); err != nil {
			klog.Errorf("Failed to stop clear manager: %v", err)
			return err
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		klog.Errorf("Failed to stop message service: %v", err)
		return err
	}

	if err := s.db.Close(); err != nil {
		klog.Errorf("Failed to close database connection: %v", err)
		return err
	}

	klog.Info("All message service components stopped successfully")
	return nil
}

func (s *messageServiceImpl) SendSync(ctx context.Context, msg *model.Message) (string, error) {
	klog.V(4).Infof("Sending sync message to topic %s with key %s", msg.Topic, msg.Key)
	id, err := s.producerManager.SendSync(ctx, msg)
	if err != nil {
		klog.Errorf("Failed to send sync message: %v", err)
		return "", err
	}
	klog.V(4).Infof("Successfully sent sync message with ID %s", id)
	return id, nil
}

func (s *messageServiceImpl) SendAsync(ctx context.Context, msg *model.Message, callback func(string, error)) error {
	klog.V(4).Infof("Sending async message to topic %s with key %s", msg.Topic, msg.Key)
	wrappedCallback := func(id string, err error) {
		if err != nil {
			klog.Errorf("Async message send failed: %v", err)
		} else {
			klog.V(4).Infof("Successfully sent async message with ID %s", id)
		}
		if callback != nil {
			callback(id, err)
		}
	}
	return s.producerManager.SendAsync(ctx, msg, wrappedCallback)
}

func (s *messageServiceImpl) GroupSubscribe(ctx context.Context, topic string, group string, handler MessageHandler) error {
	klog.Infof("Setting up group subscription for topic %s, group %s", topic, group)
	err := s.consumerManager.Consume(ctx, topic, group, handler)
	if err != nil {
		klog.Errorf("Failed to set up group subscription: %v", err)
		return err
	}
	klog.Infof("Successfully set up group subscription for topic %s, group %s", topic, group)
	return nil
}

func (s *messageServiceImpl) BroadcastSubscribe(ctx context.Context, topic string, handler MessageHandler) error {
	broadcastGroup := "__broadcast__" + uuid.New().String()
	klog.Infof("Setting up broadcast subscription for topic %s with group %s", topic, broadcastGroup)
	err := s.consumerManager.Consume(ctx, topic, broadcastGroup, handler)
	if err != nil {
		klog.Errorf("Failed to set up broadcast subscription: %v", err)
		return err
	}
	klog.Infof("Successfully set up broadcast subscription for topic %s", topic)
	return nil
}
