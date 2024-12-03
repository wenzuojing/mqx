CREATE TABLE IF NOT EXISTS `%s` (
    `offset` BIGINT PRIMARY KEY AUTO_INCREMENT,
    `message_id` VARCHAR(64),
    `tag` VARCHAR(256),
    `key` VARCHAR(256),
    `body` BLOB,
    `born_time` DATETIME NOT NULL,
    UNIQUE KEY `uk_message_id` (`message_id`),
    KEY `idx_tag` (`tag`)
) ENGINE = InnoDB 