package com.yw.flink.example.javacases.case18_cep;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * Flink SQL - Within 使用
 * 案例：读取kafka 通话日志数据，匹配多次通话失败后通话成功的基站信息，限制在5秒内进行模式匹配
 */
public class Case16_WithIn {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //设置自动watermark推进
        tableEnv.getConfig().set("table.exec.source.idle-timeout", "5000");

        //读取Kafka 基站日志数据，通过SQL DDL方式
        //001,181,182,busy,1000,1
        tableEnv.executeSql("create table station_log_tbl(" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint," +
                "   rowtime as TO_TIMESTAMP_LTZ(call_time,3)," +
                "   watermark for rowtime as rowtime - interval '2' seconds" +
                ") with (" +
                "   'connector'='kafka'," +
                "   'topic'='station_log-topic'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id'='testgroup'," +
                "   'scan.startup.mode'='latest-offset'," +
                "   'format'='csv'" +
                ")");

        //读取Kafka基站日志数据，匹配基站平均通话时长小于10s的事件
        TableResult result = tableEnv.executeSql("select " +
                "   T.sid," +
                "   T.start_dt," +
                "   T.dt " +
                "from station_log_tbl " +
                " MATCH_RECOGNIZE (" +
                "   partition by sid " +
                "   order by rowtime " +
                "   MEASURES " +
                "       FIRST(A.rowtime) as start_dt," +
                "       B.rowtime as dt" +
                "   ONE ROW PER MATCH " +
                "   AFTER MATCH SKIP TO LAST B" +
                "   PATTERN (A+ B) WITHIN INTERVAL '5' SECONDS" +
                "   DEFINE " +
                "       A as A.call_type = 'fail'," +
                "       B as B.call_type = 'success' " +
                " ) AS T");

        result.print();
    }
}
