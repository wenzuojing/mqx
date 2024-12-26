SELECT `group`, `topic`, `instance_id`, `hostname`, `active`, `heartbeat` 
FROM `mqx_consumer_instances` 
WHERE `topic` = ? 
{{if .Group}}
AND `group` = ?
{{end}}
