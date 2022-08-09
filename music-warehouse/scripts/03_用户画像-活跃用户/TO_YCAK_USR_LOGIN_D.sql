CREATE EXTERNAL TABLE `TO_YCAK_USR_LOGIN_D` (
    `ID` int,
    `UID` int,
    `MID` int,
    `LOGIN_TM` string,
    `LOGOUT_TM` string,
    `MODE_TYPE` int
) PARTITIONED BY (`data_dt` string)
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://node01/user/hive/warehouse/music.db/TO_YCAK_USR_LOGIN_D';