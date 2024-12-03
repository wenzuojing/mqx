package template

import (
	_ "embed"
)

// Topic related SQL statements
//
//go:embed sql/topic/create_topic_meta_table.sql
var CreateTopicMetaTable string

//go:embed sql/topic/get_topic_meta.sql
var GetTopicMeta string

//go:embed sql/topic/insert_topic_meta.sql
var InsertTopicMeta string

// Consumer related SQL statements
//
//go:embed sql/consumer/create_consumer_offsets_table.sql
var CreateConsumerOffsetsTable string

//go:embed sql/consumer/insert_consumer_offset.sql
var InsertConsumerOffset string

//go:embed sql/consumer/update_consumer_instance_id.sql
var UpdateConsumerInstanceId string

//go:embed sql/consumer/update_consumer_offset.sql
var UpdateConsumerOffset string

//go:embed sql/consumer/get_consumer_offset.sql
var GetConsumerOffset string

//go:embed sql/consumer/get_consumer_partition.sql
var GetConsumerPartition string

//go:embed sql/consumer/create_consumer_instances_table.sql
var CreateConsumerInstancesTable string

//go:embed sql/consumer/update_consumer_instance_heartbeat.sql
var UpdateConsumerInstanceHeartbeat string

//go:embed sql/consumer/insert_consumer_instance_heartbeat.sql
var InsertConsumerInstanceHeartbeat string

//go:embed sql/consumer/update_consumer_instance_unactive.sql
var UpdateConsumerInstanceUnactive string

//go:embed sql/consumer/delete_unactive_consumer_instance.sql
var DeleteUnactiveConsumerInstance string

//go:embed sql/consumer/get_active_consumer_instances.sql
var GetActiveConsumerInstances string

// Delay message related SQL statements
//
//go:embed sql/delay/create_delay_message_table.sql
var CreateDelayMessageTable string

//go:embed sql/delay/insert_delay_message.sql
var InsertDelayMessage string

//go:embed sql/delay/get_ready_delay_messages.sql
var GetReadyDelayMessages string

//go:embed sql/delay/delete_delay_message.sql
var DeleteDelayMessage string

//go:embed sql/lock/get_lock.sql
var GetLock string

//go:embed sql/lock/release_lock.sql
var ReleaseLock string

// Message related SQL statements
//
//go:embed sql/message/create_message_table.sql
var CreateMessageTableTemplate string

//go:embed sql/message/insert_message.sql
var InsertMessageTemplate string

//go:embed sql/message/get_messages.sql
var GetMessagesTemplate string

//go:embed sql/message/get_max_offset.sql
var GetMaxOffsetTemplate string

//go:embed sql/message/delete_messages.sql
var DeleteMessages string

//go:embed sql/topic/get_topic_meta_list.sql
var GetTopicMetaList string
