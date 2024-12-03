UPDATE mqx_consumer_instances 
SET heartbeat = NOW(), active = TRUE
WHERE `group` = ? AND `topic` = ? AND instance_id = ?

