CREATE EXTERNAL TABLE `TW_SONG_RSI_D`(
 `PERIOD` string,
 `NBR` string, 
 `NAME` string, 
 `RSI` string, 
 `RSI_RANK` int
 )
PARTITIONED BY (data_dt string)
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t' 
LOCATION 'hdfs://mycluster/user/hive/warehouse/music.db/TW_SONG_RSI_D';