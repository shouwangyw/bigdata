package com.yw.flink.example.javacases.case18_cep;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * Flink SQL  CEP
 * 案例：读取Kafka 中的基站通话日志数据，找出连续两次通话失败后通话又成功的数据。
 */
public class Case13_SqlCEP {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //设置自动watermark推进
        tableEnv.getConfig().set("table.exec.source.idle-timeout","5000");

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

        //SQL CEP
        //001 fail
        //001 fail
        //001 success
        TableResult result = tableEnv.executeSql("select " +
                "   T.sid," +
                "   T.first_fail_dt," +
                "   T.second_fail_dt," +
                "   T.success_dt " +
                "from station_log_tbl " +
                " MATCH_RECOGNIZE (" +
                "   partition by sid " +
                "   order by rowtime " +
                "   MEASURES " +
                "       FIRST(A.rowtime) as first_fail_dt," +
                "       FIRST(A.rowtime,1) as second_fail_dt," +
                "       LAST(B.rowtime) as success_dt" +
                "   ONE ROW PER MATCH " +
                "   AFTER MATCH SKIP TO LAST B" +
                "   PATTERN (A{2} B) " +
                "   DEFINE " +
                "       A as A.call_type = 'fail'," +
                "       B as B.call_type = 'success'" +
                " ) AS T");

        result.print();


    }
}
