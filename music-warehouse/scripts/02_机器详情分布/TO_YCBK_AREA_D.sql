CREATE EXTERNAL TABLE `TO_YCBK_AREA_D` (
    `CTY_ID` int,
    `AREA_ID` int,
    `AREA` string
) ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://node01/user/hive/warehouse/music.db/TO_YCBK_AREA_D';