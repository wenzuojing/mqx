package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wenzuojing/mqx"
)

// This example demonstrates the delay queue functionality:
//
// 1. Producer sends messages with different delays (0s, 2s, 5s, 10s)
// 2. Consumer subscribes to the topic
// 3. Messages arrive after their specified delay time
// 4. The delay queue processes messages asynchronously without blocking
//
// Run this example with a MySQL instance available at localhost:3306.

func main() {
	dsn := "root:mysql@tcp(localhost:3306)/mqx_dev?charset=utf8mb4&parseTime=True&loc=Local"

	cfg := mqx.NewConfig()
	cfg.DSN = dsn
	cfg.DefaultPartitionNum = 4
	cfg.DelayInterval = time.Second // check delay queue every second
	cfg.PullingInterval = time.Second
	cfg.EnableConsole = false

	mq, err := mqx.NewMQX(cfg)
	if err != nil {
		panic(err)
	}
	defer mq.Close(context.Background())

	topic := "delay-demo-topic"
	group := "delay-demo-group"

	// --- Consumer ---
	err = mq.GroupSubscribe(context.TODO(), topic, group, func(msg *mqx.MessageView) error {
		receivedAt := time.Now().Format("15:04:05.000")
		fmt.Printf("[%s] ✅ Received: key=%s, partition=%d, body=%s\n",
			receivedAt, msg.Key, msg.Partition, string(msg.Body))
		return nil
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("=== Consumer started, sending delayed messages... ===")
	fmt.Println()

	// Give the consumer a moment to initialize
	time.Sleep(time.Second * 2)

	// --- Producer ---
	// Send messages with different delays
	messages := []struct {
		key   string
		body  string
		delay time.Duration
	}{
		{"instant-1", "no delay - should arrive immediately", 0},
		{"instant-2", "no delay - should arrive immediately", 0},
		{"delay-2s", "2 second delay", 2 * time.Second},
		{"delay-5s", "5 second delay", 5 * time.Second},
		{"delay-10s", "10 second delay", 10 * time.Second},
		{"delay-15s", "15 second delay", 15 * time.Second},
	}

	sentAt := time.Now()
	fmt.Printf("[%s] Sending messages with different delays...\n", sentAt.Format("15:04:05.000"))
	fmt.Println()

	for _, m := range messages {
		msg := mqx.NewMessage().
			WithTopic(topic).
			WithKey(m.key).
			WithBody([]byte(m.body)).
			WithDelay(m.delay)

		id, err := mq.SendSync(context.TODO(), msg)
		if err != nil {
			fmt.Printf("Failed to send %s: %v\n", m.key, err)
			continue
		}

		now := time.Now().Format("15:04:05.000")
		delayStr := "instant"
		if m.delay > 0 {
			delayStr = fmt.Sprintf("delay=%v", m.delay)
		}
		fmt.Printf("[%s] 📤 Sent: key=%s, %s, messageId=%s\n", now, m.key, delayStr, id)
	}

	fmt.Println()
	fmt.Println("=== Messages sent, watching delivery... ===")
	fmt.Println()
	fmt.Println("Expected behavior:")
	fmt.Println("  - instant-1, instant-2: arrive immediately")
	fmt.Println("  - delay-2s: arrives ~2 seconds after send time")
	fmt.Println("  - delay-5s: arrives ~5 seconds after send time")
	fmt.Println("  - delay-10s: arrives ~10 seconds after send time")
	fmt.Println("  - delay-15s: arrives ~15 seconds after send time")
	fmt.Println()
	fmt.Println("Note: Actual delivery time depends on DelayInterval (1s)")
	fmt.Println("      Messages are checked every second when their delay expires.")
	fmt.Println()
	fmt.Println("Press Ctrl+C to exit")
	fmt.Println()

	// Wait for user interrupt
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("\nShutting down...")
}
