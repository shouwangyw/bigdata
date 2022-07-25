CREATE EXTERNAL TABLE `TO_YCBK_MAC_STORE_MAP_D`(
 `STORE_ID` int, 
 `MID` int, 
 `PRDCT_TYPE` int, 
 `ADMINID` int, 
 `CREAT_TM` string
)
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t' 
LOCATION 'hdfs://mycluster/user/hive/warehouse/data/machine/TO_YCBK_MAC_STORE_MAP_D';