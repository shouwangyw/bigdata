CREATE EXTERNAL TABLE `TO_YCAK_USR_APP_D`(
 `UID` int, 
 `REG_MID` int, 
 `GDR` string, 
 `BIRTHDAY` string,
 `MSISDN` string,
 `LOC_ID` int,
 `REG_TM` string,
 `USR_EXP` string,
 `LEVEL` int,
 `APP_ID` string 
 )
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t' 
LOCATION 'hdfs://mycluster/user/hive/warehouse/data/user/TO_YCAK_USR_APP_D';