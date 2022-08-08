CREATE EXTERNAL TABLE `TO_YCAK_USR_LOC_D`(
 `ID` int, 
 `UID` int, 
 `LAT` string, 
 `LNG` string,
 `DATETIME` string,
 `MID` string
 )
PARTITIONED BY (data_dt string)
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t' 
LOCATION 'hdfs://mycluster/user/hive/warehouse/data/user/TO_YCAK_USR_LOC_D';