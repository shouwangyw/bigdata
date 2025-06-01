package com.yw.flink.example.javacases.case17_connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL - 流式读取Hive分区数据
 */
public class Case11_FlinkRTReadHive {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());

        //创建Hive catalog
        tableEnv.executeSql("create catalog myhive with (" +
                "   'type'='hive'," +
                "   'default-database'='default'," +
                "   'hive-conf-dir'='.hiveconf'" +
                ")");

        //使用hive catalog
        tableEnv.useCatalog("myhive");

        //查询数据
        tableEnv.executeSql("select * from rt_hive_tbl " +
                "/*+ OPTIONS(" +
                "   'streaming-source.enable'='true'," +
                "   'streaming-source.monitor-interval' = '2 second'," +
                "   'streaming-source.consume-start-offset'='1970-01-01 08:00:00'" +
                ") */").print();
    }
}
