package com.yw.flink.example.javacases.case18_cep;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * Flink中勉强模式和贪婪模式
 * 案例:Pattern（A B* C）匹配通话事件，
 * A模式表示通话时间大于10秒事件，
 * B模式表示通话时间小于15秒事件，
 * C模式表示通话时长大于12秒事件
 */
public class Case15_SqlGreedy {
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
                "   T.dt," +
                "   T.duration " +
                "from station_log_tbl " +
                " MATCH_RECOGNIZE (" +
                "   partition by sid " +
                "   order by rowtime " +
                "   MEASURES " +
                "      C.rowtime as dt," +
                "      C.duration as duration  " +
                "   AFTER MATCH SKIP PAST LAST ROW" +
                "   PATTERN (A B*? C) " +
                "   DEFINE " +
                "       A as A.duration > 10," +
                "       B as B.duration < 15," +
                "       C as C.duration > 12 " +
                " ) AS T");

        result.print();

    }
}
