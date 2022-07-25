CREATE EXTERNAL TABLE `TO_YCAK_MAC_D`(
 `MID` int, 
 `SRL_ID` string, 
 `HARD_ID` string, 
 `SONG_WHSE_VER` string, 
 `EXEC_VER` string, 
 `UI_VER` string, 
 `IS_ONLINE` string, 
 `STS` int, 
 `CUR_LOGIN_TM` string, 
 `PAY_SW` string, 
 `LANG` int, 
 `SONG_WHSE_TYPE` int, 
 `SCR_TYPE` int)
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t' 
LOCATION 'hdfs://mycluster/user/hive/warehouse/data/machine/TO_YCAK_MAC_D';