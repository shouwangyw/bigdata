package com.yw.spark.example.structedstreaming.cases

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * kafka source
  */
object Case20_KafkaSource {
  def main(args: Array[String]): Unit = {
    //1.创建对象
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("Error")

    //2.读取Kafka 数据
    val df: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node1:9092,node2:9092,node3:9092")
      .option("subscribe", "ststopic")
      .load()

    //3.获取key value 数据
    val result: Dataset[(String, String)] = df.selectExpr(
      "cast (key as string)", "cast (value as string)").as[(String, String)]

    val query: StreamingQuery = result.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()
    query.awaitTermination()
  }
}
