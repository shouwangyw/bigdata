package com.yw.iceberg.example

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @author yangwei
  */
object SparkIcebergDQL2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      // 指定hadoop catalog，catalog名称为hadoop_prod
      .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://node01:8020/spark_iceberg")
      .getOrCreate()

    import spark.implicits._

    // 合并Iceberg表的数据文件
//    /******************* 1. 创建iceberg表表my_test *******************/
//    spark.sql("create table if not exists hadoop_prod.default.my_test(id int,name string,age int) using iceberg")
//
//    /******************* 2. 向表my_test中插入一批数据 *******************/
//    val df: DataFrame = spark.read.textFile(this.getClass.getClassLoader.getResource("nameinfo.txt").getPath)
//      .map(line => {
//        val arr: Array[String] = line.split(",")
//        (arr(0).toInt, arr(1), arr(2).toInt)
//      }).toDF("id", "name", "age")
//    df.writeTo("hadoop_prod.default.my_test").append()

//    /******************* 3. 合并小文件数据 *******************/
    val catalog = new HadoopCatalog(new Configuration(), "hdfs://node01:8020/spark_iceberg")
    val table: Table = catalog.loadTable(TableIdentifier.of("default", "my_test"))
//    SparkActions.get().rewriteDataFiles(table)
//      .filter(Expressions.greaterThanOrEqual("id", 1))
//      .option("target-file-size-bytes", "10240") // 10KB
//      .execute()

    /******************* 4. 删除历史快照 *******************/
    table.expireSnapshots().expireOlderThan(1659658697197L).commit()

    spark.close()
  }
}
