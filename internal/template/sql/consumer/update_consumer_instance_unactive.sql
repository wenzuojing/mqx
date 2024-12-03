UPDATE mqx_consumer_instances 
SET active = FALSE
WHERE `group` = ? AND `topic` = ? AND instance_id = ?

