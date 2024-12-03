INSERT INTO mqx_delay_messages (
    `message_id`,
    `topic`,
    `key`,
    `tag`,
    `body`,
    `born_time`,
    `delay_time`
) VALUES (
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?
);
