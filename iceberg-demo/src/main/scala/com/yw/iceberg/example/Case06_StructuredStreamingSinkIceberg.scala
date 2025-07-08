package com.yw.iceberg.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import java.util.concurrent.TimeUnit

/**
 * 编写StructuredStreaming读取Kafka数据实时写入Iceberg
 *
 * @author yangwei
 */
object Case06_StructuredStreamingSinkIceberg {
  System.setProperty("HADOOP_USER_NAME", "root")

  def main(args: Array[String]): Unit = {
    // 1. 准备对象
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName)
      .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
      .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://mycluster/spark_iceberg")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    // 2. 创建iceberg表
    spark.sql(
      """
        | create table if not exists hadoop_prod.default.ods_user_log(
        | current_day string
        | ,user_id string
        | ,page_id string
        | ,channel string
        | ,action string
        | ) using iceberg
        |""".stripMargin)

    val checkpointPath = "hdfs://mycluster/iceberg_tbl_checkpoint"
    val bootstrapServers = "node1:9092,node2:9092,node3:9092"
    val topic = "test-iceberg-topic"

    // 3. 读取Kafka数据
    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("auto.offset.reset", "latest")
      .option("group.id", "group-iceberg")
      .option("subscribe", topic)
      .load()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val resDf = df.selectExpr("cast(key as string)", "cast(value as string)")
      .as[(String, String)].toDF("id", "data")

    val transDf = resDf.withColumn("current_day", split(col("data"), "\t")(0))
      .withColumn("ts", split(col("data"), "\t")(1))
      .withColumn("user_id", split(col("data"), "\t")(2))
      .withColumn("page_id", split(col("data"), "\t")(3))
      .withColumn("channel", split(col("data"), "\t")(4))
      .withColumn("action", split(col("data"), "\t")(5))
      .select("current_day", "user_id", "page_id", "channel", "action")

    // 4. 流式写入iceberg表
    val query = transDf.writeStream.format("iceberg")
      .outputMode("append")
      //      // 每分钟触发一次
      //      .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
      // 每10s触发一次
      .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .option("path", "hadoop_prod.default.ods_user_log")
      .option("fanout-enabled", "true")
      .option("checkpointLocation", checkpointPath)
      .start()

    query.awaitTermination()
  }
}
