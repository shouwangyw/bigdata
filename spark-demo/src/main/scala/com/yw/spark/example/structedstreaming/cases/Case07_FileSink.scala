package com.yw.spark.example.structedstreaming.cases

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 读取Socket数据，将数据写入到csv文件
  */
object Case07_FileSink {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    val result: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node5")
      .option("port", 9999)
      .load()

    val query: StreamingQuery = result.writeStream
      .format("csv")
      .option("path", ".data/csvdir")
      .option("checkpointLocation", s".checkpoint/${System.currentTimeMillis()}")
      .start()

    query.awaitTermination()
  }
}
