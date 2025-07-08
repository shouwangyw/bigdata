package com.yw.iceberg.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用Flink SQL创建iceberg表并写入数据
 * @author yangwei
 */
public class Case04_FlinkSqlWriteIceberg {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(1000);

        // 1. 创建Catalog
        tableEnv.executeSql("create catalog hadoop_iceberg with (" +
                "'type'='iceberg'," +
                "'catalog-type'='hadoop'," +
                "'warehouse'='hdfs://mycluster/flink_iceberg')");

        // 2. 使用当前Catalog
        tableEnv.useCatalog("hadoop_iceberg");

        // 3. 创建数据库
        tableEnv.executeSql("create database iceberg_db");

        // 4. 使用数据库
        tableEnv.useDatabase("iceberg_db");

        // 5. 创建iceberg表flink_iceberg_tbl1
        tableEnv.executeSql("create table hadoop_iceberg.iceberg_db.flink_iceberg_tbl1(id int,name string,age int,loc string) partitioned by (loc)");

        // 6. 写入数据到表flink_iceberg_tbl2
        tableEnv.executeSql("insert into hadoop_iceberg.iceberg_db.flink_iceberg_tbl1 values(1,'zs',18,'beijing'),(2,'ls',19,'shanghai'),(3,'ww',20,'guangzhou')");
    }
}
