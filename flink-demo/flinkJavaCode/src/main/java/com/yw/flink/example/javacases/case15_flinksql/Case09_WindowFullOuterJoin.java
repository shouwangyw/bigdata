package com.yw.flink.example.javacases.case15_flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * Flink SQL - WindowJoin - fullOuterJoin
 * 案例：读取Kafka 两个topic数据形成表，进行Window fullOuterJoin
 */
public class Case09_WindowFullOuterJoin {
    public static void main(String[] args) {

        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());

        //设置watermark自动推进
        tableEnv.getConfig().set("table.exec.source.idle-timeout","5000");

        //读取kafka left-topic 数据，DDL方式设置:1,zs,18,1000
        tableEnv.executeSql("create table left_tbl(" +
                "   id int," +
                "   name string," +
                "   age int," +
                "   dt bigint," +
                "   rowtime as TO_TIMESTAMP_LTZ(dt,3)," +
                "   watermark for rowtime as rowtime - interval '2' seconds" +
                ") with (" +
                "   'connector'='kafka'," +
                "   'topic'='left-topic'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id'='testgroup'," +
                "   'scan.startup.mode'='latest-offset'," +
                "   'format'='csv'" +
                ")");

        //读取kafka right-topic 数据，DDL方式设置:1,zs,100,2000
        tableEnv.executeSql("create table right_tbl(" +
                "   id int," +
                "   name string," +
                "   score int," +
                "   dt bigint," +
                "   rowtime as TO_TIMESTAMP_LTZ(dt,3)," +
                "   watermark for rowtime as rowtime - interval '2' seconds" +
                ") with (" +
                "   'connector'='kafka'," +
                "   'topic'='right-topic'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id'='testgroup'," +
                "   'scan.startup.mode'='latest-offset'," +
                "   'format'='csv'" +
                ")");

        //SQL 方式实现 window fullOuterJoin
        TableResult result = tableEnv.executeSql("" +
                "select " +
                "   L.id," +
                "   L.name," +
                "   L.age," +
                "   R.score," +
                "   COALESCE(L.window_start,R.window_start) as window_start," +
                "   COALESCE(L.window_end,R.window_end) as window_end " +
                "from " +
                "   (select * from TABLE(TUMBLE(TABLE left_tbl,DESCRIPTOR(rowtime),INTERVAL '5' SECONDS))) as L " +
                "full outer join " +
                "   (select * from TABLE(TUMBLE(TABLE right_tbl,DESCRIPTOR(rowtime),INTERVAL '5' SECONDS))) as R " +
                "on L.id=R.id and L.window_start=R.window_start and L.window_end=R.window_end");


        result.print();
    }
}
