package main

import (
	"context"
	"fmt"
	"time"

	"github.com/wenzuojing/mqx"
)

func main() {

	cfg := mqx.NewConfig()
	cfg.DSN = "root:123456@tcp(localhost:3306)/mqx_dev?charset=utf8mb4&parseTime=True&loc=Local"
	cfg.HeartbeatInterval = time.Second * 5
	cfg.DefaultPartitionNum = 16
	mq, err := mqx.NewMQX(cfg)
	if err != nil {
		panic(err)
	}
	mq.GroupSubscribe(context.TODO(), "test-topic", "test-group", func(msg *mqx.MessageView) error {
		fmt.Printf("topic: %s, group: %s, partition: %d, key: %s, body: %s\n", msg.Topic, msg.Group, msg.Partition, msg.Key, string(msg.Body))
		return nil
	})

	time.Sleep(time.Second * 1000)

}
