CREATE EXTERNAL TABLE `TO_YCBK_CITY_D`(
 `PRVC_ID` int, 
 `CTY_ID` int,
 `CTY` string
)
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t' 
LOCATION 'hdfs://mycluster/user/hive/warehouse/data/machine/TO_YCBK_CITY_D';