INSERT INTO mqx_delay_messages (
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
) VALUES (
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?
);
