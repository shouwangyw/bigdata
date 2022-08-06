CREATE EXTERNAL TABLE IF NOT EXISTS `TW_SINGER_RSI_D`(
    `PERIOD` int,
    `SINGER_ID` string,
    `SINGER_NAME` string,
    `RSI` string,
    `RSI_RANK` int
) PARTITIONED BY (data_dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://node01/user/hive/warehouse/music.db/TW_SINGER_RSI_D';