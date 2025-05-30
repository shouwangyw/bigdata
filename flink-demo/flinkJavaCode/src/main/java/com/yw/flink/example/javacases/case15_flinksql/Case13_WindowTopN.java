package com.yw.flink.example.javacases.case15_flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL - Window topn - row_number() over(...)
 * 案例：读取Kafka 基站日志数据形成表，每5秒设置滚动窗口，统计每个基站每个窗口内的通话时长最小的top2
 */
public class Case13_WindowTopN {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        //设置watermark 自动推进
        tableEnv.getConfig().set("table.exec.source.idle-timeout","5000");

        tableEnv.executeSql("create table station_log_tbl(" +
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
                "   'topic'='station_log-topic'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id'='testgroup'," +
                "   'scan.startup.mode'='latest-offset'," +
                "   'format'='csv'" +
                ")");

        //window top2
        Table result = tableEnv.sqlQuery("" +
                "select " +
                "   t2.window_start,t2.window_end,t2.sid,t2.duration,t2.rk " +
                "from (" +
                "   select " +
                "       t1.window_start,t1.window_end,t1.sid,t1.duration," +
                "       row_number() over (" +
                "           partition by t1.window_start,t1.window_end,t1.sid " +
                "           order by t1.duration asc" +
                "       ) as rk " +
                "   from (" +
                "       select window_start,window_end,sid,duration " +
                "       from TABLE(TUMBLE(TABLE station_log_tbl, DESCRIPTOR(rowtime),INTERVAL '5' SECONDS))" +
                "   ) t1" +
                ") t2 " +
                "where t2.rk <=2");

        result.execute().print();
    }
}
