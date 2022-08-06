CREATE EXTERNAL TABLE `TO_YCBK_MAC_ADMIN_MAP_D` (
    `MID` int,
    `MAC_NM` string,
    `PKG_NUM` int,
    `PKG_NM` string,
    `INV_RATE` double,
    `AGE_RATE` double,
    `COM_RATE` double,
    `PAR_RATE` double,
    `DEPOSIT` double,
    `SCENE_PRVC_ID` string,
    `SCENE_CTY_ID` string,
    `SCENE_AREA_ID` string,
    `SCENE_ADDR` string,
    `PRDCT_TYPE` string,
    `SERIAL_NUM` string,
    `HAD_MPAY_FUNC` int,
    `IS_ACTV` int,
    `ACTV_TM` string,
    `ORDER_TM` string,
    `GROUND_NM` string
) ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://node01/user/hive/warehouse/music.db/TO_YCBK_MAC_ADMIN_MAP_D';