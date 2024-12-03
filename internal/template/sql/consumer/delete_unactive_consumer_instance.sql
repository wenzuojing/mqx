DELETE FROM mqx_consumer_instances 
WHERE active = FALSE OR heartbeat < DATE_SUB(NOW(), INTERVAL 2 MINUTE) 