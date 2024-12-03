CREATE TABLE IF NOT EXISTS mqx_consumer_instances (
    `group` VARCHAR(256),
    `topic` VARCHAR(256),
    `instance_id` VARCHAR(256),
    `hostname` VARCHAR(256),
    `active` BOOLEAN,
    `heartbeat` DATETIME,
    PRIMARY KEY(`group`, `topic`, `instance_id`)
) ENGINE = InnoDB 