CREATE EXTERNAL TABLE `TW_CNSM_BRIEF_D`(
 `ID` int, 
 `TRD_ID` string, 
 `UID` string, 
 `MID` int,
 `PRDCD_TYPE` int,
 `PAY_TYPE` int,
 `ACT_TM` string,
 `PKG_ID` int,
 `COIN_PRC` int,
 `COIN_CNT` int,
 `UPDATE_TM` string,
 `ORDR_ID` string,
 `ACTV_NM` string,
 `PKG_PRC` int,
 `PKG_DSCNT` int,
 `CPN_TYPE` int,
 `ABN_TYP` int
 )
PARTITIONED BY (data_dt string)
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t' 
LOCATION 'hdfs://mycluster/user/hive/warehouse/data/user/TW_CNSM_BRIEF_D';