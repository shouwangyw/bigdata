package com.yw.spark.example.structedstreaming.cases

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * 实时读取scoket数据，对数据按照事件时间进行划分窗口统计wordcount
  * 1641780000000 zhangsan,lisi,maliu,zhangsan
  * 1641780002000 zhangsan,lisi,wangwu
  * 1641780005000 lisi,maliu,lisi
  * 1641780010000 zhangsan,lisi
  * 1641780003000 wangwu,zhangsan
  */
object Case13_WindowOnEventTime {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("Error")

    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node5")
      .option("port", 9999)
      .load()

    // 处理数据，将数据中的时间列转换成时间戳类型： 1641780000000 zhangsan,lisi,maliu,zhangsan
    val tsAndWordsDF: DataFrame = df.as[String].flatMap(line => {
      val ts: String = line.split(" ")(0)
      val arr: mutable.ArraySeq[(Timestamp, String)] = line.split(" ")(1).split(",").map(word => {
        (new Timestamp(ts.toLong), word)
      })
      arr
    }).toDF("timestamp", "word")

    import org.apache.spark.sql.functions._
    //设置窗口
    val transDF: DataFrame = tsAndWordsDF.groupBy(window($"timestamp", "10 seconds", "3 seconds"),
      $"word"
    ).count()

    transDF.printSchema()

    val result: DataFrame = transDF.map(row => {
      val startTime: Timestamp = row.getStruct(0).getTimestamp(0)
      val endTime: Timestamp = row.getStruct(0).getTimestamp(1)
      val word: String = row.getString(1)
      val count: Long = row.getLong(2)

      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      (sdf.format(startTime.getTime), sdf.format(endTime.getTime), word, count)
    }).toDF("start", "end", "word", "count")

    val query: StreamingQuery = result.orderBy("start", "end").writeStream
      .format("console")
      .outputMode("complete")
      .start()

    query.awaitTermination()
  }

}
