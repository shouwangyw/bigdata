CREATE EXTERNAL TABLE `TO_YCAK_USR_LOGIN_D`(
 `ID` int, 
 `UID` int, 
 `MID` int, 
 `LOGIN_TM` string,
 `LOGOUT_TM` string,
 `MODE_TYPE` int
 )
PARTITIONED BY (`data_dt` string)
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t' 
LOCATION 'hdfs://mycluster/user/hive/warehouse/data/user/TO_YCAK_USR_LOGIN_D';