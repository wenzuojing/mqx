SELECT `topic`,`group`,  `partition`,`offset`, `instance_id` 
FROM `mqx_consumer_offsets` 
WHERE `topic` = ? 
{{if .Group}}
AND `group` = ?
{{end}}
