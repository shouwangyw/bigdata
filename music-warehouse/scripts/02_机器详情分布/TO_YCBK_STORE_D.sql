CREATE EXTERNAL TABLE `TO_YCBK_STORE_D` (
    `ID` int,
    `STORE_NM` string,
    `TAG_ID` string,
    `TAG_NM` string,
    `SUB_TAG_ID` string,
    `SUB_TAG_NM` string,
    `PRVC_ID` string,
    `CTY_ID` string,
    `AREA_ID` string,
    `ADDR` string,
    `GROUND_NM` string,
    `BUS_TM` string,
    `CLOS_TM` string,
    `SUB_SCENE_CATGY_ID` string,
    `SUB_SCENE_CATGY_NM` string,
    `SUB_SCENE_ID` string,
    `SUB_SCENE_NM` string,
    `BRND_ID` string,
    `BRND_NM` string,
    `SUB_BRND_ID` string,
    `SUB_BRND_NM` string
) ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://node01/user/hive/warehouse/music.db/TO_YCBK_STORE_D';