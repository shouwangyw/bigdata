package com.yw.spark.example.sql

import org.apache.spark.sql.SparkSession

/**
  *
  * @author yangwei
  */
object SparkOperateIceberg {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
    // 指定hive catalog, catalog名称为hive_prod
      .config("spark.sql.catalog.hive_prod", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hive_prod.type", "hive")
      .config("spark.sql.catalog.hive_prod.uri", "thrift://node03:9083")
      .config("iceberg.engine.hive.enabled", "true")
      // 指定hadoop catalog，catalog名称为hadoop_prod
      .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://node01:8020/spark_iceberg")
      .getOrCreate()

    // 1. 创建表: hive_prod:指定catalog名称, default:指定Hive中存在的库, test: 创建的iceberg表名
    spark.sql(
      """
        | create table if not exists hive_prod.default.test(id int, name string, age int) using iceberg
      """.stripMargin)

//    // 2. 插入数据
//    spark.sql(
//      """
//        | insert into hive_prod.default.test values(1, 'zhangsan', 23),(1, 'lisi', 14),(1, 'wangwu', 35)
//      """.stripMargin)
//
//    // 3. 查询数据
//    spark.sql(
//      """
//        | select * from hive_prod.default.test
//      """.stripMargin).show()
//
//    // 4. 删除表
//    spark.sql(
//      """
//        | drop table hive_prod.default.test
//      """.stripMargin)
//
//    spark.close()
  }
}
