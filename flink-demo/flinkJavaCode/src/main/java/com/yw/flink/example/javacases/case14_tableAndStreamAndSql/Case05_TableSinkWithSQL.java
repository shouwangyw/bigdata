package com.yw.flink.example.javacases.case14_tableAndStreamAndSql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL 将数据写出到外部文件系统
 */
public class Case05_TableSinkWithSQL {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.getConfig().getConfiguration().setLong("execution.checkpointing.interval", 5000L);
        //通过SQL DDL 来创建kafka 表
        tableEnv.executeSql("create table  station_log_tbl (" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint" +
                ") with (" +
                "   'connector' = 'kafka'," +
                "   'topic'='station_log-topic'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id'='testgroup'," +
                "   'scan.startup.mode'='latest-offset'," +
                "   'format'='csv'" +
                ")");

        //通过SQL DDL 方式创建 filesystem表
        tableEnv.executeSql("create table CsvSinkTable (" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint" +
                ") with (" +
                "   'connector' = 'filesystem'," +
                "   'path'='.data/flinkdata/output'," +
                "   'sink.rolling-policy.check-interval'='2s'," +
                "   'sink.rolling-policy.rollover-interval'='10s'," +
                "   'format'='csv'," +
                "   'csv.field-delimiter'='|'" +
                ")");

        //执行sql 将结果写出到文件系统
        tableEnv.executeSql("insert into CsvSinkTable select * from station_log_tbl");
    }
}
