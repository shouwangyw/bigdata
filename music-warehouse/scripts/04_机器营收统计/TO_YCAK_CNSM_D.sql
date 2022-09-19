CREATE EXTERNAL TABLE `TO_YCAK_CNSM_D`(
    `ID` int,
    `MID` int,
    `PRDCD_TYPE` int,
    `PAY_TYPE` int,
    `PKG_ID` int,
    `PKG_NM` string,
    `AMT` int,
    `CNSM_ID` string,
    `ORDR_ID` string,
    `TRD_ID` string,
    `ACT_TM` string,
    `UID` int,
    `NICK_NM` string,
    `ACTV_ID` int,
    `ACTV_NM` string,
    `CPN_TYPE` int,
    `CPN_TYPE_NM` string,
    `PKG_PRC` int,
    `PKG_DSCNT` int,
    `ORDR_TYPE` int,
    `BILL_DT` int
) PARTITIONED BY (data_dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://node01/user/hive/warehouse/music.db/TO_YCAK_CNSM_D';