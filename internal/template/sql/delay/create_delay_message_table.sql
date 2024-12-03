CREATE TABLE IF NOT EXISTS mqx_delay_messages (
    `id` BIGINT PRIMARY KEY AUTO_INCREMENT,
    `message_id` VARCHAR(64),
    `topic` VARCHAR(256) NOT NULL,
    `key` VARCHAR(256),
    `tag` VARCHAR(256),
    `body` TEXT NOT NULL,
    `born_time` DATETIME NOT NULL,
    `delay_time` DATETIME NOT NULL,
    INDEX `idx_delay_time` (`delay_time`)
) ENGINE=InnoDB;
