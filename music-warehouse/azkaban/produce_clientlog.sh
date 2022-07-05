#!/bin/bash
currentDate = `date -d today +"%Y%m%d"`
if [ x"$1" = x ]; then
	echo "====使用自动生成的今天日期===="
else
	echo "====使用 Azkaban 传入的日期===="
	currentDate = $1
fi
echo "日期为: $currentDate"
ssh root@node01 > /bigdata/data/music_project/log/produce_clientlog.txt 2>&1 <<aabbcc
cd /bigdata/install/spark-2.3.1/bin
sh ./spark-submit --master yarn-client --class com.yw.musichw.ods.ProduceClientLog /bigdata/data/music_project/musicwh-1.0.0-SNAPSHOT-jar-with-dependencies.jar $currentDate
exit
aabbcc

echo "all done!"