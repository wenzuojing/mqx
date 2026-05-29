# Async Retry via Delay Queue - Design Spec

## Problem

The current `callHandler` retry mechanism in `partition_consumer.go` blocks the entire partition consumer goroutine during retry waits. With default configuration (`RetryTimes=3`, `RetryInterval=3s`, `RetryMaxInterval=30s`), a single failing message can block partition consumption for up to 39 seconds due to synchronous exponential backoff.

## Solution

Replace the blocking inline retry loop with an asynchronous retry mechanism using the existing delay queue infrastructure. When a message handler fails:

1. The message is immediately sent to the delay queue with an incremented retry count and exponential backoff delay
2. The consumer offset is advanced, unblocking the partition
3. When the delay fires, the message is transferred back to the original topic and partition
4. The consumer picks it up naturally on the next poll cycle
5. If max retries are exhausted, the message goes to the dead letter queue (DLQ)

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Retry delivery | Back to original topic | Simplest approach, reuses existing consumption path |
| First attempt | Inline (no retry) | First call is synchronous; only failures go to delay queue |
| Retry tracking | Extend delay table schema | Explicit, queryable, minimal schema change |
| Partition assignment | Same partition | Preserves message ordering within partition |
| Retry count propagation | Add `retry_count` to message table | Retry state travels with the message, no extra lookups |

## Overall Flow

```
consume() fetches batch from mqx_messages_{topic}_{partition}
  |
  v
for each msg:
  handler(msg) -- single attempt, no blocking retry
    |
    +--> success: advance offset, continue
    |
    +--> fail:
          |
          +--> msg.RetryCount >= maxRetries-1?
          |     YES --> save to DLQ ({topic}_dead), advance offset
          |     NO  --> delayManager.AddRetry(msg, group, partition, retryCount+1, backoff)
          |             advance offset, continue (partition NOT blocked)
          |
[delay manager polling loop - every DelayInterval]
  |
  v
SELECT retry messages WHERE delay_time <= NOW()
  |
  v
for each retry message:
  INSERT INTO mqx_messages_{topic}_{partition} (..., retry_count) VALUES (..., N)
  DELETE FROM mqx_delay_messages WHERE id = ?
  |
  v
[consumer picks up the message on next poll, flow repeats]
```

## Schema Changes

### Delay table: `mqx_delay_messages`

```sql
ALTER TABLE mqx_delay_messages
  ADD COLUMN `retry_count` INT NOT NULL DEFAULT 0,
  ADD COLUMN `original_group` VARCHAR(256) DEFAULT NULL,
  ADD COLUMN `original_partition` INT DEFAULT NULL;
```

- `retry_count`: how many times this message has been retried. User-initiated delay messages have `retry_count=0`.
- `original_group`: the consumer group that triggered the retry. Needed so the correct handler processes the message when it returns.
- `original_partition`: the original partition number, ensuring the message returns to the same partition.

### Message table: `mqx_messages_{topic}_{partition}`

```sql
ALTER TABLE mqx_messages_{topic}_{partition}
  ADD COLUMN `retry_count` INT NOT NULL DEFAULT 0;
```

Newly created tables include this column automatically. Existing tables require `ALTER TABLE`.

### Model changes

```go
// model/message.go
type Message struct {
    MessageID  string        `json:"messageId"`
    BornTime   time.Time     `json:"bornTime"`
    Topic      string        `json:"topic"`
    Key        string        `json:"key"`
    Tag        string        `json:"tag"`
    Body       []byte        `json:"body"`
    Partition  int           `json:"partition"`
    Offset     int64         `json:"offset"`
    Delay      time.Duration `json:"delay"`
    RetryCount int           `json:"retryCount"` // NEW
}

type DelayMessage struct {
    ID                int64  `json:"id"`
    Message
    RetryCount        int    `json:"retryCount"`        // NEW
    OriginalGroup     string `json:"originalGroup"`     // NEW
    OriginalPartition int    `json:"originalPartition"` // NEW (distinct from Message.Partition)
}
```

### New retry model

```go
type RetryMessage struct {
    Message
    RetryCount        int
    OriginalGroup     string
    OriginalPartition int
    Delay             time.Duration
}
```

## SQL Template Changes

### `insert_delay_message.sql`

```sql
INSERT INTO mqx_delay_messages (
    message_id, topic, key, tag, body, born_time, delay_time,
    retry_count, original_group, original_partition
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
```

### `insert_message.sql`

```sql
INSERT INTO `%s` (message_id, tag, key, body, born_time, retry_count)
VALUES (?, ?, ?, ?, ?, ?);
```

### `select_messages.sql`

```sql
SELECT message_id, tag, key, body, born_time, offset, retry_count
FROM `%s` WHERE offset > ? ORDER BY offset ASC LIMIT ?;
```

### `get_ready_delay_messages.sql`

```sql
SELECT id, message_id, topic, key, tag, body, born_time, delay_time,
       retry_count, original_group, original_partition
FROM mqx_delay_messages
WHERE delay_time <= NOW()
ORDER BY born_time ASC LIMIT 100;
```

## Code Changes

### `partition_consumer.go`

**Remove** the blocking `callHandler` retry loop. Replace with a single handler invocation:

```go
func (p *partitionConsumer) callHandler(msg *model.Message) error {
    return p.handler(msg)
}
```

**Update `consume()` loop:**

```go
for _, msg := range msgs {
    if err := p.callHandler(msg); err != nil {
        if msg.RetryCount >= p.cfg.RetryTimes-1 {
            // Max retries exhausted -> DLQ
            klog.Errorf("Message %s exhausted retries (%d), sending to DLQ: %v",
                msg.MessageID, p.cfg.RetryTimes, err)
            _, dlqErr := p.factory.GetMessageManager().SaveMessage(ctx, &model.Message{
                MessageID: msg.MessageID,
                Topic:     msg.Topic + "_dead",
                Partition: msg.Partition,
                Key:       msg.Key,
                Tag:       msg.Tag,
                BornTime:  msg.BornTime,
                Body:      msg.Body,
            })
            if dlqErr != nil {
                klog.Errorf("Failed to save to DLQ: %v", dlqErr)
            }
        } else {
            // Schedule async retry via delay queue
            backoff := p.cfg.RetryInterval * (1 << uint(msg.RetryCount))
            if backoff > p.cfg.RetryMaxInterval || backoff <= 0 {
                backoff = p.cfg.RetryMaxInterval
            }
            klog.V(4).Infof("Scheduling retry for message %s (attempt %d/%d) in %v",
                msg.MessageID, msg.RetryCount+1, p.cfg.RetryTimes, backoff)
            retryErr := p.factory.GetDelayManager().AddRetry(ctx, &model.RetryMessage{
                Message:           *msg,
                RetryCount:        msg.RetryCount + 1,
                OriginalGroup:     p.group,
                OriginalPartition: msg.Partition,
                Delay:             backoff,
            })
            if retryErr != nil {
                klog.Errorf("Failed to schedule retry for message %s: %v", msg.MessageID, retryErr)
                // Fallback to DLQ to prevent message loss
                p.factory.GetMessageManager().SaveMessage(ctx, &model.Message{
                    MessageID: msg.MessageID,
                    Topic:     msg.Topic + "_dead",
                    Partition: msg.Partition,
                    Key:       msg.Key,
                    Tag:       msg.Tag,
                    BornTime:  msg.BornTime,
                    Body:      msg.Body,
                })
            }
        }

        // Advance offset (both DLQ and retry paths)
        if !isBroadcast {
            if updErr := p.updateConsumerOffset(ctx, p.group, p.topic, p.partition, p.instanceID, msg.Offset); updErr != nil {
                klog.Errorf("Failed to advance offset: %v", updErr)
            }
        } else {
            _broadcastOffset = msg.Offset + 1
        }
        continue
    }

    // Success path - advance offset as before
    if isBroadcast {
        _broadcastOffset = msg.Offset + 1
    } else {
        if err := p.updateConsumerOffset(ctx, p.group, p.topic, p.partition, p.instanceID, msg.Offset); err != nil {
            klog.Errorf("Failed to update consumer offset: %v", err)
            break
        }
    }
}
```

### `DelayManager` interface

Add `AddRetry` method:

```go
// interfaces/delay_manager.go
type DelayManager interface {
    Add(ctx context.Context, msg *model.Message) (string, error)
    AddRetry(ctx context.Context, msg *model.RetryMessage) (string, error) // NEW
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
    DeleteMessagesByTopic(ctx context.Context, topic string) error
}
```

### `delay_manager.go`

**`AddRetry` implementation:**

```go
func (d *delayManagerImpl) AddRetry(ctx context.Context, msg *model.RetryMessage) (string, error) {
    if msg.MessageID == "" {
        msg.MessageID = uuid.New().String()
    }
    delayTime := time.Now().Add(msg.Delay)
    _, err := d.db.Exec(template.InsertDelayMessage,
        msg.MessageID,
        msg.Topic,
        msg.Key,
        msg.Tag,
        msg.Body,
        msg.BornTime,
        delayTime,
        msg.RetryCount,
        msg.OriginalGroup,
        msg.OriginalPartition,
    )
    if err != nil {
        return "", fmt.Errorf("failed to insert retry message: %w", err)
    }
    return msg.MessageID, nil
}
```

**`processDelayMessages` - distinguish retry vs user delay:**

```go
for _, msg := range messages {
    if msg.RetryCount > 0 {
        // Retry message -> transfer back to original topic+partition with retry_count
        err = d.transferRetryMessage(ctx, tx, msg)
    } else {
        // User-initiated delay -> existing behavior
        err = d.factory.GetMessageManager().SaveMessageWithTx(ctx, tx, &msg.Message)
    }
    // ... existing error handling and delete logic
}
```

**`transferRetryMessage`:**

```go
func (d *delayManagerImpl) transferRetryMessage(ctx context.Context, tx *sql.Tx, msg *model.DelayMessage) error {
    return d.factory.GetMessageManager().SaveRetryMessageWithTx(ctx, tx,
        msg.Topic, msg.OriginalPartition, &msg.Message, msg.RetryCount)
}
```

### `MessageManager` interface

Add `SaveRetryMessageWithTx` method:

```go
type MessageManager interface {
    // ... existing methods
    SaveRetryMessageWithTx(ctx context.Context, tx *sql.Tx, topic string, partition int, msg *model.Message, retryCount int) error
}
```

## Exponential Backoff Formula

```
retry 0 (first attempt, inline): no delay
retry 1 (first async retry): RetryInterval * 2^0 = 3s  (default)
retry 2: RetryInterval * 2^1 = 6s
retry 3: RetryInterval * 2^2 = 12s
retry 4: RetryInterval * 2^3 = 24s
retry 5+: capped at RetryMaxInterval = 30s
```

## Edge Cases

| Scenario | Behavior |
|----------|----------|
| Retry scheduling fails (DB error) | Fallback to DLQ to prevent message loss |
| Delay manager crashes during transfer | Transaction rollback. Message stays in delay table, retried on next cycle |
| Message table doesn't exist during retry transfer | Existing DDL error handling: create table, retry transfer |
| Backoff overflow (RetryInterval * 2^N overflows) | Cap at RetryMaxInterval |
| Consumer group rebalance during retry wait | No impact. Retry message is in delay table, not tied to any consumer instance |
| Consumer stops during handler call | Handler returns error, offset not advanced. On restart, re-processes from last committed offset |
| Backward compat: existing tables without retry_count | Schema migration adds column with DEFAULT 0. Existing messages treated as first-attempt |
| Broadcast mode retries | Retry message returns to original topic/partition. Any broadcast instance picks it up |

## Testing Strategy

- **Unit test:** `callHandler` - verify single handler invocation, no retry loop
- **Unit test:** `consume()` - verify on handler failure, `AddRetry` called with correct retry_count, group, partition, backoff
- **Unit test:** `consume()` - verify when `RetryCount >= maxRetries-1`, message goes to DLQ
- **Unit test:** `delay_manager.transferRetryMessage` - verify message inserted with correct retry_count and partition
- **Unit test:** `delay_manager.AddRetry` - verify correct delay_time calculation and DB insert
- **Integration test:** Full lifecycle: message fails -> delay queue -> back to topic -> fails again -> DLQ

## Files Modified

| File | Change |
|------|--------|
| `internal/model/message.go` | Add `RetryCount` to Message, add `RetryMessage` type |
| `internal/consumer/partition_consumer.go` | Remove blocking retry loop, add delay queue integration |
| `internal/delay/delay_manager.go` | Add `AddRetry`, `transferRetryMessage`, distinguish retry vs user delay |
| `internal/interfaces/delay_manager.go` | Add `AddRetry` to interface |
| `internal/interfaces/message_manager.go` | Add `SaveRetryMessageWithTx` to interface |
| `internal/message/message_manager.go` | Implement `SaveRetryMessageWithTx` |
| `internal/template/sql.go` | Update embedded SQL references if needed |
| `internal/template/sql/delay/insert_delay_message.sql` | Add retry_count, original_group, original_partition columns |
| `internal/template/sql/delay/get_ready_delay_messages.sql` | Add new columns to SELECT |
| `internal/template/sql/delay/create_delay_message_table.sql` | Add new columns to CREATE TABLE |
| `internal/template/sql/message/insert_message.sql` | Add retry_count column |
| `internal/template/sql/message/select_messages.sql` | Add retry_count to SELECT |
| `config.go` | No changes (existing RetryTimes, RetryInterval, RetryMaxInterval reused) |
