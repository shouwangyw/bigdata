package com.yw.flink.example.javacases.case14_flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * 使用Flink SQL 查询表
 * 案例：通过读取Kafka中基站日志数据，进行分析
 */
public class Case02_QueryTableWithSQL {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //通过SQL DDL 来创建kafka 表
        tableEnv.executeSql("create table station_log_tbl (" +
                "   sid string," +
                "   call_out string," +
                "   call_in string," +
                "   call_type string," +
                "   call_time bigint," +
                "   duration bigint" +
                ") with (" +
                "   'connector' = 'kafka'," +
                "   'topic'='station_log-topic'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id'='testgroup'," +
                "   'scan.startup.mode'='latest-offset'," +
                "   'format'='csv'" +
                ")");

        //通过SQL查询数据
        TableResult result = tableEnv.executeSql("" +
                "select " +
                "   sid," +
                "   sum(duration) as total_duration " +
                "from " +
                "   station_log_tbl " +
                "where call_type = 'success' and duration >10 " +
                "group by sid");

        result.print();
    }
}
