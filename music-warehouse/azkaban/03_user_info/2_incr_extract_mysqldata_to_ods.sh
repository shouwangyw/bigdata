#!/bin/bash
currentDate=`date -d today +"%Y%m%d"`
if [ x"$1" = x ]; then
  echo "====没有导入数据的日期，输入日期===="
  exit
else
  echo "====使用导入数据的日期 ===="
  currentDate=$1
fi
echo "日期为 : $currentDate"

## 查询hive ODS层表 to_ycak_usr_login_d 中目前存在的最大的ID
maxId=`hive -e "select max(id) from music.to_ycak_usr_login_d"`
echo "Hive ODS层表 TO_YCAK_USR_LOGIN_D 最大的ID是$maxId\n"
if [ x"$maxId" = xNULL ]; then
  echo "maxId is NULL 重置为0"
  maxId=0
fi

## user_login_info 	==>> 	TO_YCAK_USR_LOGIN_D
sqoop import --connect jdbc:mysql://node01:3306/ycak?dontTrackOpenResources=true\&defaultFetchSize=10000\&useCursorFetch=true\&useUnicode=yes\&characterEncoding=utf8 --username root --password 123456 --table user_login_info --target-dir /user/hive/warehouse/music.db/TO_YCAK_USR_LOGIN_D/data_dt=${currentDate} --num-mappers 1 --fields-terminated-by '\t' --incremental append --check-column id --last-value ${maxId}

#更新Hive 分区
hive -e "alter table music.to_ycak_usr_login_d add partition(data_dt=${currentDate});"

echo "all done!"