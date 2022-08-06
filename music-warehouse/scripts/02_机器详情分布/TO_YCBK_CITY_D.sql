CREATE EXTERNAL TABLE `TO_YCBK_CITY_D` (
    `PRVC_ID` int,
    `CTY_ID` int,
    `CTY` string
) ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://node01/user/hive/warehouse/music.db/TO_YCBK_CITY_D';