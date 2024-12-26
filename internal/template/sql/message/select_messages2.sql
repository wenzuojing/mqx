SELECT 
    `message_id`, 
    `tag`, 
    `key`, 
    `body`, 
    `born_time`, 
    `offset` 
FROM `{{.TableName}}` 
WHERE `offset` > 0
{{if .MessageID}}
    AND `message_id` = ?
{{end}}
{{if .Tag}}
    AND `tag` = ?
{{end}}
ORDER BY `offset` ASC 
LIMIT ? OFFSET ?
