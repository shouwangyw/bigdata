package com.yw.spark.example.structedstreaming.cases

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Structured Streaming 读取Socket数据
  *
  * @author yangwei
  */
object Case01_SocketWordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local")
      // 设置并行度
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    // 2. 读取Socket数据
    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node5")
      .option("port", 9999)
      .load()

//    // 3. 统计wordcount: 对每行数据按照空格进行切分单词
//    val words: Dataset[String] = df.as[String]
//      .flatMap(line => line.split(" "))
//
//    // 4. 统计wordcount: 按照单词分组，对单词进行计数
//    val wordCounts: DataFrame = words.groupBy("value").count()

    // 3. 打印结果
    val query: StreamingQuery = df.writeStream
      .format("console")
      // 指定结果输出模式，默认是append，除此之外还有complete、update
      .outputMode("append")
//      .trigger(Trigger.ProcessingTime(0))
      .trigger(Trigger.Continuous("2 seconds"))
//      .trigger(Trigger.Continuous(5, TimeUnit.MINUTES))
      .option("checkpointLocation", s".checkpoint/${System.currentTimeMillis()}")
//      .trigger(Trigger.ProcessingTime("5 seconds"))
//      .trigger(Trigger.ProcessingTime(5,TimeUnit.SECONDS))
//      .trigger(Trigger.Once())
      .start()

    query.awaitTermination()
  }
}
