#!/bin/bash
ssh hadoop@node03 > /tmp/logs/music_project/user-info.log 2>&1 <<aabbcc
hostname
source /etc/profile

# ycak
## user_wechat_baseinfo 	==>> 	TO_YCAK_USR_D
sqoop import --connect jdbc:mysql://node01:3306/ycak?dontTrackOpenResources=true\&defaultFetchSize=10000\&useCursorFetch=true\&useUnicode=yes\&characterEncoding=utf8 --username root --password 123456 --table user_wechat_baseinfo --target-dir /user/hive/warehouse/music.db/TO_YCAK_USR_D/ --delete-target-dir --num-mappers 1 --fields-terminated-by '\t'
## user_alipay_baseinfo 	==>> 	TO_YCAK_USR_ALI_D
sqoop import --connect jdbc:mysql://node01:3306/ycak?dontTrackOpenResources=true\&defaultFetchSize=10000\&useCursorFetch=true\&useUnicode=yes\&characterEncoding=utf8 --username root --password 123456 --table user_alipay_baseinfo --target-dir /user/hive/warehouse/music.db/TO_YCAK_USR_ALI_D/ --delete-target-dir --num-mappers 1 --fields-terminated-by '\t'
## user_qq_baseinfo 	==>> 	TO_YCAK_USR_QQ_D
sqoop import --connect jdbc:mysql://node01:3306/ycak?dontTrackOpenResources=true\&defaultFetchSize=10000\&useCursorFetch=true\&useUnicode=yes\&characterEncoding=utf8 --username root --password 123456 --table user_qq_baseinfo --target-dir /user/hive/warehouse/music.db/TO_YCAK_USR_QQ_D/ --delete-target-dir --num-mappers 1 --fields-terminated-by '\t'
## user_app_baseinfo 	==>> 	TO_YCAK_USR_APP_D
sqoop import --connect jdbc:mysql://node01:3306/ycak?dontTrackOpenResources=true\&defaultFetchSize=10000\&useCursorFetch=true\&useUnicode=yes\&characterEncoding=utf8 --username root --password 123456 --table user_app_baseinfo --target-dir /user/hive/warehouse/music.db/TO_YCAK_USR_APP_D/ --delete-target-dir --num-mappers 1 --fields-terminated-by '\t'

exit

aabbcc

echo "all done!"