CREATE EXTERNAL TABLE `TW_MAC_STAT_D`(
 `MID` int, 
 `MAC_NM` string, 
 `PRDCT_TYPE` string, 
 `STORE_NM` int,
 `BUS_MODE` string,
 `PAY_SW` string,
 `SCENCE_CATGY` string,
 `SUB_SCENCE_CATGY` string,
 `SCENE` string,
 `SUB_SCENE` string,
 `BRND` string,
 `SUB_BRND` string,
 `PRVC` string,
 `CTY` string,
 `AREA` string,
 `AGE_ID` string,
 `INV_RATE` string,
 `AGE_RATE` string,
 `COM_RATE` string,
 `PAR_RATE` string,
 `PKG_ID` string,
 `PAY_TYPE` string,
 `CNSM_USR_CNT` string,
 `REF_USR_CNT` string,
 `NEW_USR_CNT` string,
 `REV_ORDR_CNT` string,
 `REF_ORDR_CNT` string,
 `TOT_REV` string,
 `TOT_REF` string
 )
PARTITIONED BY (data_dt string)
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t' 
LOCATION 'hdfs://mycluster/user/hive/warehouse/data/machine/TW_MAC_STAT_D';