package com.yw.flink.example.javacases.case17_connector;


import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL 读取Hive metastore 持久化的数据表
 */
public class Case09_ReadFlinkGenericTable {
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
        tableEnv.executeSql("select * from flink_kafka_tbl " +
                "/*+ OPTIONS('scan.startup.mode'='earliest-offset') */").print();

    }
}
