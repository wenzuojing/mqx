SELECT `topic`, `partition_num`, `retention_days` 
FROM `mqx_topic_metas` 
WHERE `topic` = ?
