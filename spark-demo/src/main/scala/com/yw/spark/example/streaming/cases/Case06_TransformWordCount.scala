package com.yw.spark.example.streaming.cases

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 获取每一个批次中单词出现次数最多的前三位
  *
  * @author yangwei
  */
object Case06_TransformWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // 1. 创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    // 2. 创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // 3. 接收Socket数据
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    // 4. 对数据进行处理
    val result: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_, 1))
      .reduceByKey(_ + _)

    // 5. 对DStream进行transform操作
    val sortedDstream: DStream[(String, Int)] = result.transform(rdd => {
      val sortedRDD: RDD[(String, Int)] = rdd.sortBy(_._2, false)
      val top3: Array[(String, Int)] = sortedRDD.take(3)
      top3.foreach(println)
      sortedRDD
    })

    // 6. 打印结果
    sortedDstream.print()

    // 7. 开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
