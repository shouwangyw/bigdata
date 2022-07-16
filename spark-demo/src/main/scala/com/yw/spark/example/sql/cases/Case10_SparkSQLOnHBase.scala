package com.yw.spark.example.sql.cases

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  *
  * @author yangwei
  */
object Case10_SparkSQLOnHBase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val hconf: Configuration = HBaseConfiguration.create
    hconf.set(HConstants.ZOOKEEPER_QUORUM, "node01:2181,node02:2181,node03:2181")
    val hbaseContext = new HBaseContext(spark.sparkContext, hconf)

    // 定义映射的 catalog
    val catalog: String = "{\"table\":{\"namespace\":\"default\",\"name\":\"spark_hbase\"},\"rowkey\":\"key\",\"columns\":{\"f0\":{\"cf\":\"rowkey\",\"col\":\"key\",\"type\":\"string\"},\"f1\":{\"cf\":\"info\",\"col\":\"addr\",\"type\":\"string\"},\"f2\":{\"cf\":\"info\",\"col\":\"age\",\"type\":\"boolean\"},\"f3\":{\"cf\":\"info\",\"col\":\"name\",\"type\":\"string\"}}}";

    // 读取HBase数据
    val ds: DataFrame = spark.read.format("org.apache.hadoop.hbase.spark")
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .load()
    ds.show(10)

    val catalogCopy: String = catalog.replace("spark_hbase", "spark_hbase_out")
    // 数据写入HBase
    ds.write.format("org.apache.hadoop.hbase.spark")
      .option(HBaseTableCatalog.tableCatalog, catalogCopy)
      .mode(SaveMode.Overwrite)
      .save()
  }
}
