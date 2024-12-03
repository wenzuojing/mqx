SELECT `offset` 
FROM mqx_consumer_offsets 
WHERE `group` = ? 
AND `topic` = ? 
AND `partition` = ? 
AND `instance_id` = ?