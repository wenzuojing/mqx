package main

import (
	"context"
	"fmt"
	"time"

	"github.com/wenzuojing/mqx"
)

func main() {
	//集成测试代码
	cfg := mqx.NewConfig()
	cfg.DefaultPartitionNum = 16
	mq, err := mqx.NewMQX(cfg)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 100; i++ {
		msg := mqx.NewMessage().WithTopic("test-topic").WithKey(fmt.Sprintf("%d", i)).WithBody([]byte(fmt.Sprintf("test message %d", i)))
		id, err := mq.SendSync(context.TODO(), msg)
		if err != nil {
			panic(err)
		}
		fmt.Printf("id: %s\n", id)
	}

	time.Sleep(time.Second * 100)
}
