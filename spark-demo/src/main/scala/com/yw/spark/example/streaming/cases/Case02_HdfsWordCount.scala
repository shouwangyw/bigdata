package com.yw.spark.example.streaming.cases

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming接收hdfs数据实现单词统计
  *
  * @author yangwei
  */
object Case02_HdfsWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // 1. 创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    // 2. 创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // 3. 监控hdfs目录的数据
    val textFileStream: DStream[String] = ssc.textFileStream("hdfs://node01:8020/data")

    // 4. 对数据进行处理
    val result: DStream[(String, Int)] = textFileStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    // 5. 打印结果
    result.print()

    // 6. 开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
