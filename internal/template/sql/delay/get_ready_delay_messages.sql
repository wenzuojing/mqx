SELECT
    `id`,
    `message_id`,
    `topic`,
    `key`,
    `tag`,
    `body`,
    `born_time`,
    `delay_time`,
    `retry_count`,
    `original_group`,
    `original_partition`
FROM mqx_delay_messages
WHERE `delay_time` <= NOW()
ORDER BY `born_time` ASC
LIMIT 100;
