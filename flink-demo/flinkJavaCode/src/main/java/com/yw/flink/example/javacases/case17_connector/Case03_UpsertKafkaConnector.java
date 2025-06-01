package com.yw.flink.example.javacases.case17_connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Table Connector - Upsert Kafka Connector
 * 案例：读取Kafka 中数据，进行聚合，写出到Kafka
 */
public class Case03_UpsertKafkaConnector {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        //推进watermark
        tableEnv.getConfig().set("table.exec.source.idle-timeout","5000");

        //读取Kafka t1,t2 topic数据， ddl方式
        tableEnv.executeSql("create table KafkaSourceTable (" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint," +
                "   rowtime as to_timestamp_ltz(call_time,3)," +
                "   watermark for rowtime as rowtime - interval '2' seconds " +
                ") with (" +
                "   'connector'='kafka'," +
                "   'topic'='t1'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id'='testgroup'," +
                "   'scan.startup.mode'='latest-offset'," +
                "   'value.format'='csv'" +
                ")");

        //upsert kafka 写出变更日志流数据
        tableEnv.executeSql("create table KafkaSinkTable(" +
                "   sid string," +
                "   sum_dur bigint," +
                "   PRIMARY KEY (sid) NOT ENFORCED" +
                ") with (" +
                "   'connector'='upsert-kafka'," +
                "   'topic'='t3'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'key.format'='csv'," +
                "   'value.format'='csv'" +
                ")");

        //写出数据
        tableEnv.executeSql("insert into KafkaSinkTable " +
                "select sid, sum(duration) as sum_dur from KafkaSourceTable group by sid");
    }
}
