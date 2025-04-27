package com.yw.spark.example.structedstreaming.cases

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @author yangwei
  */
object Case21_KafkaSink {
  def main(args: Array[String]): Unit = {
    // 1. 创建对象
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("Error")

    // 2. 读取Kafka 数据
    val df1: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
      .option("subscribe", "ststopic")
      .load()

    // 3. 获取key value 数据
    val result: DataFrame = df1.selectExpr("cast (key as string)", "cast (value as string)")
      .as[(String, String)]
      .flatMap(tp => {
        tp._2.split(",")
      }).toDF("word")
      .groupBy("word")
      .count()

    result.printSchema()

    // 4. 将数据写出到Kafka
    val query: StreamingQuery = result.map(row => {
      val word: String = row.getString(0)
      val count: Long = row.getLong(1)
      word + "-" + count
    }).writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
      .option("topic", "result-topic")
      .option("checkpointLocation", s".checkpoint/${System.currentTimeMillis()}")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    query.awaitTermination()
  }

}
