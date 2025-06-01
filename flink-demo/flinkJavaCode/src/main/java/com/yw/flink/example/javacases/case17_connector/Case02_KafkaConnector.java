package com.yw.flink.example.javacases.case17_connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink Table Connector - Kafka Connector
 * 案例：读取kafka 多个topic数据，将数据写出到Kafka
 * t1,t2 ->t3
 */
public class Case02_KafkaConnector {
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
                "   key_str string," +//key列
                "   rowtime as to_timestamp_ltz(call_time,3)," +
                "   watermark for rowtime as rowtime - interval '2' seconds " +
                ") with (" +
                "   'connector'='kafka'," +
                "   'topic'='t1;t2'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id'='testgroup'," +
                "   'scan.startup.mode'='latest-offset'," +
                "   'key.format'='csv'," +
                "   'key.fields'='key_str'," +
                "   'value.fields-include'='EXCEPT_KEY'," +
                "   'value.format'='csv'" +
                ")");

        //kafka sink 表
        tableEnv.executeSql("create table KafkaSinkTable(" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint" +
                ") with (" +
                "   'connector'='kafka'," +
                "   'topic'='t3'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'key.format'='csv'," +
                "   'key.fields'='sid'," +
                "   'value.fields-include'='ALL'," +
                "   'value.format'='csv'" +
                ")");

        //SQL
        tableEnv.executeSql("insert into KafkaSinkTable select sid,call_out,call_in,call_type,call_time,duration from KafkaSourceTable");
    }
}
