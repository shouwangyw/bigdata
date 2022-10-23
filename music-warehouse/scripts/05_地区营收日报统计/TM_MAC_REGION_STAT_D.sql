CREATE EXTERNAL TABLE `TM_MAC_REGION_STAT_D`(
    `PRVC` string,
    `CTY` string,
    `MAC_CNT` int,
    `MAC_REV` DECIMAL(10,2),
    `MAC_REF` DECIMAL(10,2),
    `MAC_REV_ORDR_CNT` int,
    `MAC_REF_ORDR_CNT` int,
    `MAC_CNSM_USR_CNT` int,
    `MAC_REF_USR_CNT` int
) PARTITIONED BY (DATA_DT string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://node01/user/hive/warehouse/music.db/TM_MAC_REGION_STAT_D';