CREATE EXTERNAL TABLE `TW_MAC_LOC_D`(
 `MID` int, 
 `X` string, 
 `Y` string, 
 `CNT` int,
 `ADDER` string,
 `PRVC` string,
 `CTY` string,
 `CTY_CD` string,
 `DISTRICT` string,
 `AD_CD` string,
 `TOWN_SHIP` string,
 `TOWN_CD` string,
 `NB_NM` string,
 `NB_TP` string,
 `BD_NM` string,
 `BD_TP` string,
 `STREET` string,
 `STREET_NB` string,
 `STREET_LOC` string,
 `STREET_DRCTION` string,
 `STREET_DSTANCE` string,
 `BUS_INFO` string
 )
PARTITIONED BY (data_dt string)
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t' 
LOCATION 'hdfs://mycluster/user/hive/warehouse/data/machine/TW_MAC_LOC_D';