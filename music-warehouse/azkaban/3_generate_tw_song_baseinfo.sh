#!/bin/bash
ssh hadoop@node01 > /tmp/logs/music_project/music-rsi.log 2>&1 <<aabbcc
hostname
source /etc/profile
spark-submit --master yarn-client --class com.yw.musichw.eds.content.GenerateTwSongBaseinfoD \
    /bigdata/data/music_project/musicwh-1.0.0-SNAPSHOT-jar-with-dependencies.jar
exit
aabbcc

echo "all done!"