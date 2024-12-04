UPDATE mqx_topic_metas 
SET partition_num = ?, retention_days = ?
WHERE topic = ? 