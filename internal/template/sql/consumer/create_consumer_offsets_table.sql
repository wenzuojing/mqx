CREATE TABLE IF NOT EXISTS `mqx_consumer_offsets` (
    `group` VARCHAR(256),
    `topic` VARCHAR(256),
    `partition` INT,
    `offset` BIGINT,
    `instance_id` VARCHAR(256),
    PRIMARY KEY(`group`, `topic`, `partition`)
) ENGINE = InnoDB 