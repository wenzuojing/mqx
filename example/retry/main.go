package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/wenzuojing/mqx"
)

// This example demonstrates the async retry mechanism:
//
// 1. Producer sends messages to "retry-demo-topic"
// 2. Consumer handler intentionally fails certain messages to trigger retries
// 3. Failed messages are sent to the delay queue and re-delivered after backoff
// 4. Other messages continue to be consumed without blocking
// 5. Messages that exhaust all retries go to the dead letter queue (retry-demo-topic_dead)
//
// Run this example with a MySQL instance available at localhost:3306.

func main() {
	dsn := "root:mysql@tcp(localhost:3306)/mqx_dev?charset=utf8mb4&parseTime=True&loc=Local"

	cfg := mqx.NewConfig()
	cfg.DSN = dsn
	cfg.DefaultPartitionNum = 4
	cfg.RetryTimes = 3                  // max 3 attempts (first + 2 retries), then DLQ
	cfg.RetryInterval = time.Second * 2 // backoff base: 2s, 4s, 8s ...
	cfg.PullingInterval = time.Second   // poll every second
	cfg.DelayInterval = time.Second     // check delay queue every second
	cfg.EnableConsole = true

	mq, err := mqx.NewMQX(cfg)
	if err != nil {
		panic(err)
	}
	defer mq.Close(context.Background())

	topic := "retry-demo-topic"
	group := "retry-demo-group"

	// --- Consumer ---
	// Track how many times each message key has been attempted
	attempts := &sync.Map{}

	err = mq.GroupSubscribe(context.TODO(), topic, group, func(msg *mqx.MessageView) error {
		key := msg.Key

		// Count attempts for this key
		count, _ := attempts.LoadOrStore(key, 0)
		attempt := count.(int) + 1
		attempts.Store(key, attempt)

		now := time.Now().Format("15:04:05.000")
		fmt.Printf("[%s] Received: key=%s, partition=%d, messageId=%s, attempt=%d\n",
			now, key, msg.Partition, msg.MessageID, attempt)

		// Simulate different failure scenarios based on key:
		//
		//   key="msg-fail-0"  -> always succeeds (normal message)
		//   key="msg-fail-2"  -> fails 2 times, succeeds on 3rd attempt (within retry limit)
		//   key="msg-fail-5"  -> always fails (will be sent to DLQ after 3 attempts)
		//
		switch key {
		case "msg-fail-0":
			fmt.Printf("[%s] ✅ Success: key=%s (processed on attempt %d)\n", now, key, attempt)
			return nil

		case "msg-fail-2":
			if attempt <= 2 {
				fmt.Printf("[%s] ❌ Fail: key=%s (attempt %d, will retry)\n", now, key, attempt)
				return fmt.Errorf("simulated transient error for %s", key)
			}
			fmt.Printf("[%s] ✅ Success: key=%s (recovered on attempt %d)\n", now, key, attempt)
			return nil

		case "msg-fail-5":
			fmt.Printf("[%s] ❌ Fail: key=%s (attempt %d, permanent error)\n", now, key, attempt)
			return fmt.Errorf("simulated permanent error for %s", key)

		default:
			// Other messages succeed immediately
			fmt.Printf("[%s] ✅ Success: key=%s (processed on attempt %d)\n", now, key, attempt)
			return nil
		}
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("=== Consumer started, sending messages... ===")
	fmt.Println()

	// Give the consumer a moment to initialize
	time.Sleep(time.Second * 2)

	// --- Producer ---
	// Send messages that will trigger different retry scenarios
	messages := []struct {
		key  string
		body string
	}{
		{"msg-normal", "normal message, processed immediately"},
		{"msg-fail-0", "another normal message"},
		{"msg-fail-2", "fails 2 times, recovers on 3rd attempt"},
		{"msg-fail-5", "always fails, will go to DLQ"},
	}

	for _, m := range messages {
		msg := mqx.NewMessage().
			WithTopic(topic).
			WithKey(m.key).
			WithBody([]byte(m.body))

		id, err := mq.SendSync(context.TODO(), msg)
		if err != nil {
			fmt.Printf("Failed to send %s: %v\n", m.key, err)
			continue
		}
		now := time.Now().Format("15:04:05.000")
		fmt.Printf("[%s] Sent: key=%s, messageId=%s\n", now, m.key, id)
	}

	fmt.Println()
	fmt.Println("=== Messages sent, watching retry flow... ===")
	fmt.Println()
	fmt.Println("Expected behavior:")
	fmt.Println("  - msg-normal  -> processed immediately")
	fmt.Println("  - msg-fail-0  -> processed immediately")
	fmt.Println("  - msg-fail-2  -> fails 2x, retries via delay queue, succeeds on 3rd attempt")
	fmt.Println("  - msg-fail-5  -> fails 3x (max retries), sent to DLQ (retry-demo-topic_dead)")
	fmt.Println()
	fmt.Println("  During retries, the partition consumer is NOT blocked.")
	fmt.Println("  Other messages continue to be consumed normally.")
	fmt.Println()

	// Wait for user interrupt
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	fmt.Println("\nShutting down...")
}
