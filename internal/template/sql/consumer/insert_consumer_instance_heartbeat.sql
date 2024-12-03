INSERT INTO mqx_consumer_instances (`group`, `topic`, `instance_id`, `hostname`, `heartbeat`, `active`)
VALUES (?, ?, ?, ?, NOW(), TRUE) 
