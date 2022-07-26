CREATE EXTERNAL TABLE IF NOT EXISTS `TO_CLIENT_SONG_PLAY_OPERATE_REQ_D`(
 `SONGID` string, 
 `MID` string,
 `OPTRATE_TYPE` string, 
 `UID` string, 
 `CONSUME_TYPE` string, 
 `DUR_TIME` string, 
 `SESSION_ID` string, 
 `SONGNAME` string, 
 `PKG_ID` string, 
 `ORDER_ID` string
)
partitioned by (data_dt string)
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://mycluster/user/hive/warehouse/music.db/TO_CLIENT_SONG_PLAY_OPERATE_REQ_D';