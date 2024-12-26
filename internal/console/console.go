package console

import (
	"context"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/wenzuojing/mqx/internal/config"
	"github.com/wenzuojing/mqx/internal/interfaces"
	"github.com/wenzuojing/mqx/internal/model"
	"k8s.io/klog"
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

type Partition struct {
	Partition int                       `json:"partition"`
	Stat      *interfaces.PartitionStat `json:"stat"`
}

// SendMessageRequest represents the request structure for sending a message
type SendMessageRequest struct {
	Tag  string `json:"tag"`
	Key  string `json:"key"`
	Body string `json:"body" binding:"required"`
}

// UpdateTopicRequest represents the request structure for updating topic metadata
type UpdateTopicRequest struct {
	PartitionNum  int `json:"partitionNum" binding:"required"`
	RetentionDays int `json:"retentionDays" binding:"required"`
}

type CreateTopicRequest struct {
	Topic         string `json:"topic" binding:"required"`
	PartitionNum  int    `json:"partitionNum" binding:"required"`
	RetentionDays int    `json:"retentionDays" binding:"required"`
}

func NewConsoleServer(cfg *config.Config, factory interfaces.Factory) *ConsoleServer {
	engine := gin.Default()

	// Add CORS middleware
	corsConfig := cors.DefaultConfig()
	corsConfig.AllowAllOrigins = true
	corsConfig.AllowMethods = []string{"*"}
	corsConfig.AllowHeaders = []string{"*"}
	engine.Use(cors.New(corsConfig))

	return &ConsoleServer{
		cfg:     cfg,
		engine:  engine,
		factory: factory,
	}
}

func (s *ConsoleServer) Start(ctx context.Context) error {
	// Setup routes
	s.setupRoutes()

	// Start HTTP server
	go func() {
		if err := s.engine.Run(s.cfg.Console.Address); err != nil {
			klog.Errorf("Failed to start console server: %v", err)
		}
	}()

	return nil
}

func (s *ConsoleServer) setupRoutes() {
	// API group
	api := s.engine.Group("/api")
	{
		// Topic endpoints
		api.GET("/topics", s.listTopics)
		api.PUT("/topics/:topic", s.updateTopic)
		api.POST("/topics", s.createTopic)
		api.DELETE("/topics/:topic", s.deleteTopic)
		// Message endpoints
		api.POST("/topics/:topic/messages", s.sendMessage)

		api.GET("/topics/:topic/consumer-groups", s.listConsumerGroups)
		api.GET("/topics/:topic/consumer-groups/:group/offsets", s.listConsumerGroupOffsets)
		api.GET("/topics/:topic/partitions", s.listPartitions)

		api.GET("/messages", s.listMessages)
	}
}

func (s *ConsoleServer) listMessages(c *gin.Context) {

	var params struct {
		Topic     string `form:"topic" binding:"required"`
		Partition int    `form:"partition"`
		MessageID string `form:"messageId"`
		Tag       string `form:"tag"`
		PageNo    int    `form:"pageNo" binding:"required"`
		PageSize  int    `form:"pageSize" binding:"required"`
	}
	if err := c.ShouldBindQuery(&params); err != nil {
		c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		return
	}
	total, messages, err := s.factory.GetMessageManager().QueryMessageForPage(c.Request.Context(), params.Topic, params.Partition, params.MessageID, params.Tag, params.PageNo, params.PageSize)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"messages": messages, "total": total})
}

func (s *ConsoleServer) listConsumerGroups(c *gin.Context) {
	topic := c.Param("topic")
	if topic == "" {
		c.JSON(http.StatusOK, gin.H{"error": "topic parameter is required"})
		return
	}

	consumerInstances, err := s.factory.GetConsumerManager().GetConsumerInstances(c.Request.Context(), topic, "")
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		return
	}

	activeInstances := make([]model.ConsumerInstance, 0)
	for _, instance := range consumerInstances {
		if instance.Active && instance.Heartbeat.After(time.Now().Add(-s.cfg.HeartbeatInterval*3)) {
			activeInstances = append(activeInstances, instance)
		}
	}
	groupInstanceCount := make(map[string]int)
	for _, instance := range activeInstances {
		groupInstanceCount[instance.Group]++
	}

	partitions, err := queryTopicPartitions(c.Request.Context(), s.factory, topic)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		return
	}

	consumerOffsets, err := s.factory.GetConsumerManager().GetConsumerOffsets(c.Request.Context(), topic, "")
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		return
	}

	type ConsumerGroup struct {
		Group       string `json:"group"`
		Delay       int64  `json:"delay"`
		ClientCount int    `json:"clientCount"`
	}

	max := func(a, b int64) int64 {
		if a > b {
			return a
		}
		return b
	}

	uniqGroup := make(map[string]bool)
	for _, instance := range consumerOffsets {
		uniqGroup[instance.Group] = true
	}

	var consumerGroups []ConsumerGroup
	for group := range uniqGroup {
		consumerGroup := ConsumerGroup{
			Group: group,
		}

		if count, ok := groupInstanceCount[group]; ok {
			consumerGroup.ClientCount = count
		}

		var delay int64
		for _, offset := range consumerOffsets {
			if offset.Group != group {
				continue
			}
			for _, partition := range partitions {
				if partition.Partition == offset.Partition {
					delay += partition.Stat.MaxOffset - max(offset.Offset, partition.Stat.MinOffset)
				}
			}
		}
		consumerGroup.Delay = delay
		consumerGroups = append(consumerGroups, consumerGroup)
	}

	c.JSON(http.StatusOK, gin.H{
		"consumerGroups": consumerGroups,
	})
}

func (s *ConsoleServer) listConsumerGroupOffsets(c *gin.Context) {
	topic := c.Param("topic")
	if topic == "" {
		c.JSON(http.StatusOK, gin.H{"error": "topic parameter is required"})
		return
	}

	group := c.Param("group")
	if group == "" {
		c.JSON(http.StatusOK, gin.H{"error": "group parameter is required"})
		return
	}

	partitions, err := queryTopicPartitions(c.Request.Context(), s.factory, topic)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		return
	}

	partitionStatMap := make(map[int]*Partition)
	for _, partition := range partitions {
		partitionStatMap[partition.Partition] = &partition
	}

	consumerInstances, err := s.factory.GetConsumerManager().GetConsumerInstances(c.Request.Context(), topic, group)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		return
	}
	instanceMap := make(map[string]*model.ConsumerInstance)
	for _, instance := range consumerInstances {
		if instance.Group == group {
			instanceMap[instance.InstanceID] = &instance
		}
	}

	consumerOffsets, err := s.factory.GetConsumerManager().GetConsumerOffsets(c.Request.Context(), topic, group)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		return
	}

	partitionMap := make(map[int]*model.ConsumerOffset)
	for _, offset := range consumerOffsets {
		partitionMap[offset.Partition] = &offset
	}

	type Offset struct {
		Partition  int    `json:"partition"`
		Offset     int64  `json:"offset"`
		InstanceId string `json:"instanceId"`
		Hostname   string `json:"hostname"`
		Active     bool   `json:"active"`
		MaxOffset  int64  `json:"maxOffset"`
		MinOffset  int64  `json:"minOffset"`
	}

	var offsets []Offset
	for _, partition := range partitions {
		consumerPartition := partitionMap[partition.Partition]

		offsets = append(offsets, Offset{
			Partition: partition.Partition,
			Offset:    consumerPartition.Offset,
		})

		if instance, ok := instanceMap[consumerPartition.InstanceID]; ok {
			offsets[partition.Partition].InstanceId = instance.InstanceID
			offsets[partition.Partition].Hostname = instance.Hostname
			offsets[partition.Partition].Active = instance.Active
		}

		if stat, ok := partitionStatMap[partition.Partition]; ok {
			offsets[partition.Partition].MaxOffset = stat.Stat.MaxOffset
			offsets[partition.Partition].MinOffset = stat.Stat.MinOffset
		}

	}

	c.JSON(http.StatusOK, gin.H{"offsets": offsets})
}

func (s *ConsoleServer) listPartitions(c *gin.Context) {
	topic := c.Param("topic")
	if topic == "" {
		c.JSON(http.StatusOK, gin.H{"error": "topic parameter is required"})
		return
	}

	partitions, err := queryTopicPartitions(c.Request.Context(), s.factory, topic)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"partitions": partitions})
}

// listTopics handles the GET /api/topics request
func (s *ConsoleServer) listTopics(c *gin.Context) {
	topicMetas, err := s.factory.GetTopicManager().GetAllTopicMeta(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var topics []Topic
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, topicMeta := range topicMetas {
		topic := Topic{
			Topic:         topicMeta.Topic,
			PartitionNum:  topicMeta.PartitionNum,
			RetentionDays: topicMeta.RetentionDays,
		}

		wg.Add(topicMeta.PartitionNum)
		for i := 0; i < topicMeta.PartitionNum; i++ {
			go func(t *Topic, partition int) {
				defer wg.Done()
				stat, err := s.factory.GetMessageManager().GetPartitionStat(context.Background(), t.Topic, partition)
				if err != nil {
					klog.Errorf("Failed to get message total for topic %s partition %d: %v", t.Topic, partition, err)
					return
				}
				mu.Lock()
				t.MessageTotal += stat.Total
				mu.Unlock()
			}(&topic, i)
		}
		wg.Wait()
		topics = append(topics, topic)
	}

	c.JSON(http.StatusOK, TopicListResponse{
		Topics: topics,
		Total:  int64(len(topics)),
	})
}

// sendMessage handles the POST /api/messages request
func (s *ConsoleServer) sendMessage(c *gin.Context) {
	topic := c.Param("topic")
	if topic == "" {
		c.JSON(http.StatusOK, gin.H{"error": "topic parameter is required"})
		return
	}

	var req SendMessageRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		return
	}

	msg := &model.Message{
		Topic:    topic,
		Tag:      req.Tag,
		Key:      req.Key,
		Body:     []byte(req.Body),
		BornTime: time.Now(),
	}

	messageID, err := s.factory.GetProducerManager().SendSync(c.Request.Context(), msg)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"messageId": messageID,
	})
}

// updateTopic handles the PUT /api/topics request
func (s *ConsoleServer) updateTopic(c *gin.Context) {
	topic := c.Param("topic")
	if topic == "" {
		c.JSON(http.StatusOK, gin.H{"error": "topic parameter is required"})
		return
	}

	var req UpdateTopicRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		return
	}

	topicMeta := &model.TopicMeta{
		Topic:         topic,
		PartitionNum:  req.PartitionNum,
		RetentionDays: req.RetentionDays,
	}

	if err := s.factory.GetTopicManager().UpdateTopicMeta(c.Request.Context(), topicMeta); err != nil {
		klog.Errorf("Failed to update topic: %v", err)
		c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Topic updated successfully"})
}

// createTopic handles the POST /api/topics request
func (s *ConsoleServer) createTopic(c *gin.Context) {
	var req CreateTopicRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		return
	}

	topicMeta := &model.TopicMeta{
		Topic:         req.Topic,
		PartitionNum:  req.PartitionNum,
		RetentionDays: req.RetentionDays,
	}

	if err := s.factory.GetTopicManager().CreateTopic(c.Request.Context(), topicMeta); err != nil {
		klog.Errorf("Failed to create topic: %v", err)
		c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Topic created successfully"})
}

// deleteTopic handles the DELETE /api/topics request
func (s *ConsoleServer) deleteTopic(c *gin.Context) {
	topic := c.Param("topic")
	if topic == "" {
		c.JSON(http.StatusOK, gin.H{"error": "topic parameter is required"})
		return
	}

	if err := s.factory.GetTopicManager().DeleteTopic(c.Request.Context(), topic); err != nil {
		klog.Errorf("Failed to delete topic: %v", err)
		c.JSON(http.StatusOK, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Topic deleted successfully"})
}

func queryTopicPartitions(ctx context.Context, factory interfaces.Factory, topic string) ([]Partition, error) {

	topicMeta, err := factory.GetTopicManager().GetTopicMeta(ctx, topic)
	if err != nil {
		return nil, err
	}

	var partitions []Partition
	var wg sync.WaitGroup
	var mu sync.Mutex

	for i := 0; i < topicMeta.PartitionNum; i++ {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()
			stat, err := factory.GetMessageManager().GetPartitionStat(context.Background(), topic, partition)
			if err != nil {
				return
			}
			mu.Lock()
			partitions = append(partitions, Partition{Partition: partition, Stat: stat})
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	//排序
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].Partition < partitions[j].Partition
	})
	return partitions, nil

}
