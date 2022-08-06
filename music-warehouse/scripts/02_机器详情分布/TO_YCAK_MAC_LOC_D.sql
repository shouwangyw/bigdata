CREATE EXTERNAL TABLE `TO_YCAK_MAC_LOC_D` (
    `MID` int,
    `PRVC_ID` int,
    `CTY_ID` int,
    `PRVC` string,
    `CTY` string,
    `MAP_CLSS` string,
    `LON` string,
    `LAT` string,
    `ADDR` string,
    `ADDR_FMT` string,
    `REV_TM` string,
    `SALE_TM` string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://node01/user/hive/warehouse/music.db/TO_YCAK_MAC_LOC_D';