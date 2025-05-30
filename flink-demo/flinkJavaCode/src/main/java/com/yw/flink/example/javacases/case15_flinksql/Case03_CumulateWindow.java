package com.yw.flink.example.javacases.case15_flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL 编程 - 累积窗口
 * 案例：读取Socket基站日志数据，1天设置一个窗口统计每个基站的通话时长。5秒输出一次累积结果。
 */
public class Case03_CumulateWindow {
    public static void main(String[] args) {
        //创建环境
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());

        //设置watermark自动推进
        tableEnv.getConfig().set("table.exec.source.idle-timeout","5000");

        //读取Kafka 数据，通过SQL DDL定义结构
        tableEnv.executeSql("" +
                "create table station_log_tbl(" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint," +
                "   rowtime AS TO_TIMESTAMP_LTZ(call_time,3)," +
                "   WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECONDS " +
                ") with (" +
                "   'connector'='kafka'," +
                "   'topic'='station_log-topic'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id'= 'testgroup'," +
                "   'scan.startup.mode'='latest-offset'," +
                "   'format'='csv'" +
                ")");

        //通过TVF 设置累积窗口
        Table result = tableEnv.sqlQuery("" +
                "select " +
                "   sid," +
                "   window_start," +
                "   window_end," +
                "   sum(duration) as total_dur " +
                "from " +
                "   TABLE(" +
                "       CUMULATE(TABLE station_log_tbl, DESCRIPTOR(rowtime), INTERVAL '5' SECOND,INTERVAL '1' Day )" +
                "   ) " +
                "group by sid,window_start,window_end");

        result.execute().print();
    }
}
