CREATE EXTERNAL TABLE `TW_USR_BASEINFO_D` (
    `UID` int,
    `REG_MID` int,
    `REG_CHNL` string,
    `REF_UID` string,
    `GDR` string,
    `BIRTHDAY` string,
    `MSISDN` string,
    `LOC_ID` int,
    `LOG_MDE` string,
    `REG_DT` string,
    `REG_TM` string,
    `USR_EXP` string,
    `SCORE` int,
    `LEVEL` int,
    `USR_TYPE` string,
    `IS_CERT` string,
    `IS_STDNT` string
) PARTITIONED BY (`data_dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://node01/user/hive/warehouse/music.db/TW_USR_BASEINFO_D';