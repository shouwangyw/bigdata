package com.yw.iceberg.example;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * @author yangwei
 */
public class SparkIceberg {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName(SparkIceberg.class.getSimpleName())
                // 指定hive catalog, catalog名称为hive_prod
                .config("spark.sql.catalog.hive_prod", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.hive_prod.type", "hive")
                .config("spark.sql.catalog.hive_prod.uri", "thrift://node03:9083")
                .config("iceberg.engine.hive.enabled", "true")
                // 指定hadoop catalog，catalog名称为hadoop_prod
                .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
                .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://node01:8020/spark_iceberg")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        /** 使用HiveCatalog **/
        // 1. 创建表 hive_prod:指定catalog名称, default:指定Hive中存在的库, test: 创建的iceberg表名
        spark.sql("create table if not exists hive_prod.default.test(id int, name string, age int) using iceberg");

        // 2. 插入数据
        spark.sql("insert into hive_prod.default.test values(1, 'zhangsan', 23),(2, 'lisi', 14),(3, 'wangwu', 35)");

        // 3. 查询数据
        List<Row> rows = spark.sql("select * from hive_prod.default.test").collectAsList();
        rows.forEach(System.out::println);

        // 4. 删除表
        spark.sql("drop table hive_prod.default.test");

        /** 使用HadoopCatalog **/
        // 1. 创建表 hadoop_prod: 指定Hadoop catalog名称, default: 指定库名称, test: 创建的iceberg表名
        spark.sql("create table if not exists hadoop_prod.default.test(id int,name string,age int) using iceberg");

        // 2. 插入数据
        spark.sql("insert into hadoop_prod.default.test values(1, 'zhangsan', 23),(2, 'lisi', 14),(3, 'wangwu', 35)");

        // 3. 查询数据
        rows = spark.sql("select * from hadoop_prod.default.test").collectAsList();
        rows.forEach(System.out::println);

        // 4. 删除表
        spark.sql("drop table hadoop_prod.default.test");

        spark.close();
    }
}