CREATE EXTERNAL TABLE IF NOT EXISTS `TW_SONG_RSI_D`(
    `PERIOD` int,
    `NBR` string,
    `NAME` string,
    `RSI` string,
    `RSI_RANK` int
) PARTITIONED BY (data_dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://node01/user/hive/warehouse/music.db/TW_SONG_RSI_D';