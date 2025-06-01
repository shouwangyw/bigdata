package com.yw.flink.example.javacases.case17_connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink 通用表 - 由Hive Metastore来管理Flink 表，不能在Hive中查询使用
 */
public class Case08_FlinkGenericTable {
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

        //通过FlinkSQL ddl 方式创建Flink 表 ，读取Kafka 中数据
        tableEnv.executeSql("create table flink_kafka_tbl (" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint," +
                "   rowtime as to_timestamp_ltz(call_time,3)," +
                "   watermark for rowtime as rowtime - interval '2' seconds" +
                ") with (" +
                "   'connector'='kafka'," +
                "   'topic'='new-topic'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id'='testgroup'," +
                "   'scan.startup.mode'='latest-offset'," +
                "   'format'='csv'" +
                ")");

        //通过sql查询数据
        tableEnv.executeSql("select * from flink_kafka_tbl").print();
    }
}
