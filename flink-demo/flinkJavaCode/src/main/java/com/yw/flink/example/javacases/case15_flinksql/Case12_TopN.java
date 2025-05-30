package com.yw.flink.example.javacases.case15_flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL - 通过开窗函数实现top-n结果获取
 * 案例：读取Kafka station_log-topic中的数据形成表，按照sid分组按照通话时长排序获取每个基站通话长top2
 */
public class Case12_TopN {
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

        //设置row_number() over(...) 获取top2 数据
        Table result = tableEnv.sqlQuery("" +
                "select " +
                "   t.sid,t.duration,t.rk " +
                "from (" +
                "   select " +
                "       sid," +
                "       duration," +
                "       row_number() over (partition by sid order by duration asc ) as rk " +
                "   from station_log_tbl" +
                ") t " +
                "where t.rk <=2");

        result.execute().print();
    }
}
