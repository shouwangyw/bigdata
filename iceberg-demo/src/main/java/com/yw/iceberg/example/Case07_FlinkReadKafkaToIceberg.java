package com.yw.iceberg.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用Flink SQL API 读取Kafka数据实时写入Iceberg表
 * @author yangwei
 */
public class Case07_FlinkReadKafkaToIceberg {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(1000);

        // 1. 创建catalog
        tableEnv.executeSql("create catalog hadoop_iceberg with (" +
                "'type'='iceberg'," +
                "'catalog-type'='hadoop'," +
                "'warehouse'='hdfs://mycluster/flink_iceberg')");

        // 2. 创建iceberg表flink_iceberg_tbl2
        tableEnv.executeSql("create table if not exists hadoop_iceberg.iceberg_db.flink_iceberg_tbl2(id int,name string,age int,loc string) partitioned by (loc)");

        // 3. 创建Kafka connector，连接消费Kafka中数据
        tableEnv.executeSql("create table kafka_input_table(" +
                "id int," +
                "name varchar," +
                "age int," +
                "loc varchar) with (" +
                "'connector'='kafka'," +
                "'topic'='flink-iceberg-topic'," +
                "'properties.bootstrap.servers'='node1:9092,node2:9092,node3:9092'," +
                "'scan.startup.mode'='latest-offset'," +
                "'properties.group.id'='group-flink-iceberg'," +
                "'format'='csv')");

        // 4. 配置 table.dynamic-table-options.enabled
        Configuration conf = tableEnv.getConfig().getConfiguration();
        // 支持SQL语法中的OPTIONS选项
        conf.setBoolean("table.dynamic-table-options.enabled", true);

        // 5. 写入数据到 flink_iceberg_tbl2
        tableEnv.executeSql("insert into hadoop_iceberg.iceberg_db.flink_iceberg_tbl2 select id,name,age,loc from kafka_input_table");

        // 6. 查询表数据
        TableResult tableResult = tableEnv.executeSql("select * from hadoop_iceberg.iceberg_db.flink_iceberg_tbl2" +
                "/*+ OPTIONS('streaming'='true', 'monitor-interval'='1s')*/");

        tableResult.print();
    }
}
