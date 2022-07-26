#!/bin/bash
ssh hadoop@node03 > /tmp/logs/music_project/music-rsi.log 2>&1 <<aabbcc
hostname
source /etc/profile
sqoop import --connect jdbc:mysql://node01:3306/songdb?dontTrackOpenResources=true\&defaultFetchSize=10000\&useCursorFetch=true\&useUnicode=yes\&characterEncoding=utf8 \
    --username root --password 123456 --table song --target-dir /user/hive/warehouse/music.db/TO_SONG_INFO_D/ \
    --delete-target-dir --num-mappers 1 --fields-terminated-by '\t'
exit
aabbcc

echo "all done!"