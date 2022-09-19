CREATE EXTERNAL TABLE `TO_YCAK_USR_LOC_D` (
    `ID` int,
    `UID` int,
    `LAT` string,
    `LNG` string,
    `DATETIME` string,
    `MID` string
) PARTITIONED BY (data_dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://node01/user/hive/warehouse/music.db/TO_YCAK_USR_LOC_D';