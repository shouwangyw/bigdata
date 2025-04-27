package com.yw.spark.example.structedstreaming.cases

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @author yangwei
  */
object Case06_RateSource {
  def main(args: Array[String]): Unit = {
    // 1. 创建对象
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    val result: DataFrame = spark.readStream
      .format("rate")
      // 配置每秒生成多少行数据，默认1行
      .option("rowsPerSecond", "10")
      .option("numPartitions", 5)
      .load()
    result.writeStream
      .format("console")
      .option("numRows", "100")
      .option("truncate", "false")
      .start()
      .awaitTermination()

  }
}
