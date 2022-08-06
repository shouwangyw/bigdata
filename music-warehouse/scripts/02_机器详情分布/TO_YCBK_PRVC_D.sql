CREATE EXTERNAL TABLE `TO_YCBK_PRVC_D` (
    `PRVC_ID` int,
    `PRVC` string
) ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://node01/user/hive/warehouse/music.db/TO_YCBK_PRVC_D';