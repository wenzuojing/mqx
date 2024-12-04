package console

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/wenzuojing/mqx/internal/config"
	"github.com/wenzuojing/mqx/internal/interfaces"
	"github.com/wenzuojing/mqx/internal/model"
)

type ConsoleServer struct {
	cfg     *config.Config
	engine  *gin.Engine
	factory interfaces.Factory
}

// TopicListResponse represents the response structure for topic list
type TopicListResponse struct {
	Topics []Topic `json:"topics"`
	Total  int64   `json:"total"`
}

// Topic represents a message topic
type Topic struct {
	Topic         string `json:"topic"`
	PartitionNum  int    `json:"partitionNum"`
	RetentionDays int    `json:"retentionDays"`
	MessageTotal  int64  `json:"messageTotal"`
}

// SendMessageRequest represents the request structure for sending a message
type SendMessageRequest struct {
	Topic string `json:"topic" binding:"required"`
	Tag   string `json:"tag"`
	Key   string `json:"key"`
	Body  string `json:"body" binding:"required"`
}

// UpdateTopicRequest represents the request structure for updating topic metadata
type UpdateTopicRequest struct {
	Topic         string `json:"topic" binding:"required"`
	PartitionNum  int    `json:"partitionNum" binding:"required"`
	RetentionDays int    `json:"retentionDays" binding:"required"`
}

func NewConsoleServer(cfg *config.Config) *ConsoleServer {
	engine := gin.Default()
	return &ConsoleServer{
		cfg:    cfg,
		engine: engine,
	}
}

func (s *ConsoleServer) Start(ctx context.Context) error {
	// Setup routes
	s.setupRoutes()

	// Start HTTP server
	return s.engine.Run(s.cfg.Console.Address)
}

func (s *ConsoleServer) setupRoutes() {
	// API group
	api := s.engine.Group("/api")
	{
		// Topic endpoints
		api.GET("/topics", s.listTopics)
		api.PUT("/topics", s.updateTopic)
		// Message endpoints
		api.POST("/messages", s.sendMessage)
	}
}

// listTopics handles the GET /api/topics request
func (s *ConsoleServer) listTopics(c *gin.Context) {
	topicMetas, err := s.factory.GetTopicManager().GetAllTopicMeta(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var topics []Topic

	for _, topicMeta := range topicMetas {
		topic := Topic{
			Topic:         topicMeta.Topic,
			PartitionNum:  topicMeta.PartitionNum,
			RetentionDays: topicMeta.RetentionDays,
		}

		for i := 0; i < topicMeta.PartitionNum; i++ {
			var messageTotal int64
			if messageTotal, err = s.factory.GetMessageManager().GetMessageTotal(c.Request.Context(), topicMeta.Topic, i); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			topic.MessageTotal = topic.MessageTotal + messageTotal
		}

		topics = append(topics, topic)

	}

	c.JSON(http.StatusOK, TopicListResponse{
		Topics: topics,
		Total:  int64(len(topics)),
	})
}

// sendMessage handles the POST /api/messages request
func (s *ConsoleServer) sendMessage(c *gin.Context) {
	var req SendMessageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	msg := &model.Message{
		Topic: req.Topic,
		Tag:   req.Tag,
		Key:   req.Key,
		Body:  []byte(req.Body),
	}

	messageID, err := s.factory.GetProducerManager().SendSync(c.Request.Context(), msg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"messageId": messageID,
	})
}

// updateTopic handles the PUT /api/topics request
func (s *ConsoleServer) updateTopic(c *gin.Context) {
	var req UpdateTopicRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	topicMeta := &model.TopicMeta{
		Topic:         req.Topic,
		PartitionNum:  req.PartitionNum,
		RetentionDays: req.RetentionDays,
	}

	if err := s.factory.GetTopicManager().UpdateTopicMeta(c.Request.Context(), topicMeta); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Topic updated successfully"})
}
