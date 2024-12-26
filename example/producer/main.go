package main

import (
	"context"
	"fmt"

	"github.com/wenzuojing/mqx"
)

func main() {
	//集成测试代码
	cfg := mqx.NewConfig()
	cfg.DSN = "root:123456@tcp(localhost:3306)/mqx_dev?charset=utf8mb4&parseTime=True&loc=Local"
	cfg.DefaultPartitionNum = 8
	mq, err := mqx.NewMQX(cfg)
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1000; i++ {
		msg := mqx.NewMessage().WithTopic("test-topic").WithKey(fmt.Sprintf("%d", i)).WithBody([]byte(fmt.Sprintf("test message %d", i)))
		id, err := mq.SendSync(context.TODO(), msg)
		if err != nil {
			panic(err)
		}
		fmt.Printf("id: %s\n", id)
	}
}
