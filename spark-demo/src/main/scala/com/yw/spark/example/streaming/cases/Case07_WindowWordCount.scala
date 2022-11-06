package com.yw.spark.example.streaming.cases

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 实现每隔6秒，统计12秒内的单词个数
  *
  * @author yangwei
  */
object Case07_WindowWordCount {
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
      .reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(12), Seconds(6))

    // 5. 打印结果
    result.print()

    // 6. 开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
