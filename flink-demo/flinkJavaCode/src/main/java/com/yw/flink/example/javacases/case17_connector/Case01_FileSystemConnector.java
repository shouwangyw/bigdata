package com.yw.flink.example.javacases.case17_connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Table Connector - FilesystemConnector
 * 案例：读取文件系统中数据写出到文件系统
 */
public class Case01_FileSystemConnector {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());

        //设置checkpoint
        tableEnv.getConfig().getConfiguration().setLong("execution.checkpointing.interval", 5000L);

        //读取文件系统数据 ，通过DDL方式定义
        tableEnv.executeSql("create table CsvSourceTable (" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint," +
                "   rowtime as to_timestamp_ltz(call_time,3)," +
                "   watermark for rowtime as rowtime - interval '2' seconds" +
                ") with (" +
                "   'connector'='filesystem'," +
                "   'path'='.data/flinksource'," +
                "   'format'='csv'," +
                "   'csv.field-delimiter'=','" +
                ")");

        //通过DDL 定义文件写出表
        tableEnv.executeSql("create table CsvSinkTable(" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint," +
                "   rowtime timestamp_ltz(3)" +
                ") with (" +
                "   'connector'='filesystem'," +
                "   'path'='.data/flinksink'," +
                "   'sink.rolling-policy.check-interval'='2s'," +
                "   'sink.rolling-policy.rollover-interval'='10s'," +
                "   'format'='csv'," +
                "   'csv.field-delimiter'='|'" +
                ")");

        //向外部系统写出数据
        tableEnv.executeSql("insert into CsvSinkTable " +
                "select sid,call_out,call_in,call_type,call_time,duration,rowtime from CsvSourceTable");
    }
}
