package com.yw.spark.example.structedstreaming.cases

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Structured Streaming监控目录 text格式数据
  *
  * @author yangwei
  */
object Case03_ReadTextSource {
  def main(args: Array[String]): Unit = {
    // 1. 创建对象
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("Error")

    // 2. 监控目录
    val ds: Dataset[String] = spark.readStream.textFile(".data/")

    val result: DataFrame = ds.map(line => {
      val arr: Array[String] = line.split("-")
      (arr(0).toInt, arr(1), arr(2).toInt)
    }).toDF("id", "name", "age")

    val query: StreamingQuery = result.writeStream
      .format("console")
      .start()

    query.awaitTermination()

  }
}
