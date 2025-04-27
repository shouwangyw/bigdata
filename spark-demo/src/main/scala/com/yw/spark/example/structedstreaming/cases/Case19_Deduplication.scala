package com.yw.spark.example.structedstreaming.cases

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 流数据去重
  */
object Case19_Deduplication {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._

    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node5")
      .option("port", 9998)
      .load()
      .as[String]
      .map(line => {
        val arr: Array[String] = line.split(",")
        (new Timestamp(arr(0).toLong), arr(1).toInt, arr(2), arr(3).toInt)
      }).toDF("ts", "id", "name", "score")
      .withWatermark("ts", "10 seconds")

    // 对数据进行去重
    val df2: Dataset[Row] = df.dropDuplicates("id", "name")

    // 对df2 进行统计结果
    val result: DataFrame = df2.groupBy("name").sum("score")

    // 打印结果
    result.writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }
}
