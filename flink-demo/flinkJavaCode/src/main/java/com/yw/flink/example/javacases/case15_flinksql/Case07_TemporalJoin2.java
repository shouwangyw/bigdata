package com.yw.flink.example.javacases.case15_flinksql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Flink SQL - 使用 For System_time as of 查询时态表数据
 * 案例：读取Kafka topic数据形成主表和时态表，从时态表中查询时态数据
 */
public class Case07_TemporalJoin2 {
    public static void main(String[] args) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());

        //设置watermark自动推进
        tableEnv.getConfig().set("table.exec.source.idle-timeout","5000");

        //读取Kafka - visit-topic  ： p_002,1000
        tableEnv.executeSql("create table visit_tbl(" +
                "   left_product_id string," +
                "   left_visit_time bigint," +
                "   left_rowtime as TO_TIMESTAMP_LTZ(left_visit_time,3)," +
                "   watermark for left_rowtime as left_rowtime - interval '5' seconds" +
                ") with (" +
                "   'connector'='kafka'," +
                "   'topic'='visit-topic'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id'='testgroup'," +
                "   'scan.startup.mode'='latest-offset'," +
                "   'format'='csv'" +
                ")");

        //读取Kafka - product-topic  ： 1000,p_001,电脑,3.0
        tableEnv.executeSql("create table product_tbl(" +
                "   right_dt bigint," +
                "   right_product_id string," +
                "   right_product_name string," +
                "   right_price double," +
                "   PRIMARY KEY(right_product_id) NOT ENFORCED," +
                "   right_rowtime as TO_TIMESTAMP_LTZ(right_dt,3)," +
                "   watermark for right_rowtime as right_rowtime - interval '5' seconds" +
                ") with (" +
                "   'connector'='kafka'," +
                "   'topic'='product-topic'," +
                "   'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "   'properties.group.id'='testgroup'," +
                "   'scan.startup.mode'='latest-offset'," +
                "   'format'='debezium-json'" +
                ")");

        //通过 For System_time as of 查询时态表数据
        Table result = tableEnv.sqlQuery("" +
                "select " +
                "   left_product_id," +
                "   left_visit_time," +
                "   right_product_name," +
                "   right_price " +
                "from visit_tbl " +
                "join product_tbl for system_time as of left_rowtime " +
                "on left_product_id=right_product_id");

        result.execute().print();
    }
}
