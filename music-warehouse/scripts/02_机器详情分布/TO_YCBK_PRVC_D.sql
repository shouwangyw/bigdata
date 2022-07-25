CREATE EXTERNAL TABLE `TO_YCBK_PRVC_D`(
 `PRVC_ID` int, 
 `PRVC` string
)
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t' 
LOCATION 'hdfs://mycluster/user/hive/warehouse/data/machine/TO_YCBK_PRVC_D';