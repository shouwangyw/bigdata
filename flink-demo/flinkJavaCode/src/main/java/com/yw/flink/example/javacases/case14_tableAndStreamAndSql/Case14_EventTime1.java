package com.yw.flink.example.javacases.case14_tableAndStreamAndSql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * FlinK Table API 及SQL编程 - 指定EventTime
 */
public class Case14_EventTime1 {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(
                EnvironmentSettings.newInstance().inStreamingMode().build());

        //设置自定推进watermark
        tableEnv.getConfig().set("table.exec.source.idle-timeout", "5000");

        //读取Kafka中数据，DDL方式
        tableEnv.executeSql("" +
                "create table station_log_tbl(" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string ," +
                "   call_time bigint," +
                "   duration bigint," +
                "   time_ltz AS TO_TIMESTAMP_LTZ(call_time,3)," +
                "   WATERMARK FOR time_ltz AS time_ltz - INTERVAL '2' SECOND" +
                ") with (" +
                "   'connector' = 'kafka'," +
                "   'topic'='station_log-topic'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id'='testgroup'," +
                "   'scan.startup.mode'='latest-offset'," +
                "   'format'='csv'" +
                ")");

        //设置5s一个窗口统计数据
        Table result = tableEnv.sqlQuery("" +
                "select " +
                "TUMBLE_START(time_ltz,INTERVAL '5' SECOND) AS window_start," +
                "TUMBLE_END(time_ltz,INTERVAL '5' SECOND) AS window_end," +
                "count(sid) as cnt " +
                "from station_log_tbl " +
                "group by TUMBLE(time_ltz,INTERVAL '5' SECOND)");

        result.execute().print();

    }
}
