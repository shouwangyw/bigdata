CREATE EXTERNAL TABLE `TW_SINGER_RSI_D`(
 `PERIOD` string,
 `SINGER_ID` string, 
 `SINGER_NAME` string, 
 `RSI` string, 
 `RSI_RANK` int
 )
PARTITIONED BY (data_dt string)
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t' 
LOCATION 'hdfs://mycluster/user/hive/warehouse/music.db/TW_SINGER_RSI_D';