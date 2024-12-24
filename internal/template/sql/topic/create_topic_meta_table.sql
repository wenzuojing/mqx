CREATE TABLE IF NOT EXISTS mqx_topic_metas (
    `topic` VARCHAR(256) PRIMARY KEY,
    `partition_num`  INT,
    `retention_days`  INT
) ENGINE = InnoDB 
