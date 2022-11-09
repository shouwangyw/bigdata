package com.yw.spark.example.streaming.cases

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 将WordCount案例中得到的结果通过foreachRDD保存结果到mysql中
  *
  * @author yangwei
  */
object Case10_WordCountStreamingAndSql {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // 1. 创建SparkConf对象，注意这里至少给两个线程，一个线程没办法执行
    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    // 2. 创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // 3. 接收Socket数据
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    // 4. 对数据进行处理
    val words: DStream[String] = socketTextStream.flatMap(_.split(" "))

    // 5. 对DStream进行处理，将RDD转换成DataFrame
    words.foreachRDD(rdd => {
      // 获取 SparkSession
      val sparkSession: SparkSession = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import sparkSession.implicits._
      val dataFrame: DataFrame = rdd.toDF("word")
      // 将dataFrame注册成表
      dataFrame.createOrReplaceTempView("words")
      // 统计每个单词出现的次数
      val result: DataFrame = sparkSession.sql("select word, count(*) as count from words group by word")

      // 展示结果
      result.show()
    })

    // 6. 开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
