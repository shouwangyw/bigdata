package com.yw.flink.example.javacases.case15_flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL - Over Window开窗函数
 * 案例：读取socket基站日志数据，统计每条数据往前5秒的每个基站通话总时长
 */
public class Case04_OverWindow1 {
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

        //设置 over window
        Table result = tableEnv.sqlQuery("" +
                "select " +
                "   sid," +
                "   call_time," +
                "   sum(duration) over (" +
                "       partition by sid " +
                "       order by rowtime " +
                "       range between interval '5' seconds preceding and current row" +
                "   ) as sum_dur " +
                "from station_log_tbl");

        result.execute().print();
    }
}
