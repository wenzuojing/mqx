SELECT
    `message_id`,
    `tag`,
    `key`,
    `body`,
    `born_time`,
    `offset`,
    `retry_count`
FROM `%s`
WHERE `offset` > ?
ORDER BY `offset` ASC
LIMIT ?
