SELECT 
    `message_id`, 
    `tag`, 
    `key`, 
    `body`, 
    `born_time`, 
    `offset` 
FROM `%s` 
WHERE `offset` > ? 
ORDER BY `offset` ASC 
LIMIT ? 