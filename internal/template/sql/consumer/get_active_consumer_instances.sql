SELECT `group`, `instance_id`, `hostname`, `active`, `heartbeat` 
FROM mqx_consumer_instances 
WHERE `group` = ? 
AND `topic` = ?
AND `active` = TRUE 
AND `heartbeat` > NOW() - INTERVAL 120 SECOND
