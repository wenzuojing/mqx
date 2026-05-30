CREATE TABLE IF NOT EXISTS mqx_delay_messages (
    `id` BIGINT PRIMARY KEY AUTO_INCREMENT,
    `message_id` VARCHAR(64),
    `topic` VARCHAR(256) NOT NULL,
    `key` VARCHAR(256),
    `tag` VARCHAR(256),
    `body` BLOB NOT NULL,
    `born_time` DATETIME NOT NULL,
    `delay_time` DATETIME NOT NULL,
    `retry_count` INT NOT NULL DEFAULT 0,
    INDEX `idx_delay_time` (`delay_time`)
) ENGINE=InnoDB;
