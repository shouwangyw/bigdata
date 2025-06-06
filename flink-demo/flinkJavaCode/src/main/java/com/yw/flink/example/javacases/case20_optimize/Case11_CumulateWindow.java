package com.yw.flink.example.javacases.case20_optimize;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 累积窗口使用
 */
public class Case11_CumulateWindow {
    public static void main(String[] args) {
        //创建TableEnvironment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //当某个并行度5秒没有数据输入时，自动推进watermark
        tableEnv.getConfig().set("table.exec.source.idle-timeout", "5000");


        //读取Kafka基站日志数据，通过SQL DDL方式定义表结构
        tableEnv.executeSql("" +
                "create table station_log_tbl (" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint," +
                "   time_ltz AS TO_TIMESTAMP_LTZ(call_time,3)," +
                "   WATERMARK FOR time_ltz AS time_ltz - INTERVAL '2' SECOND" +
                ") with (" +
                "   'connector' = 'kafka'," +
                "   'topic' = 'station_log-topic'," +
                "   'properties.bootstrap.servers' = 'node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id' = 'testGroup'," +
                "   'scan.startup.mode' = 'latest-offset'," +
                "   'format' = 'csv'" +
                ")");

        //SQL TumblingWindow
        Table result = tableEnv.sqlQuery("select " +
                "sid,window_start,window_end,sum(duration) as sum_dur " +
                "from TABLE(" +
                "   CUMULATE(TABLE station_log_tbl,DESCRIPTOR(time_ltz), INTERVAL '5' SECOND , INTERVAL '1' DAY)" +
                ") " +
                "group by sid,window_start,window_end");

        //打印结果
        result.execute().print();
    }
}
