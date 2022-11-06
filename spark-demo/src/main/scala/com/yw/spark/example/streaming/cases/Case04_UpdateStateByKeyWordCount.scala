package com.yw.spark.example.streaming.cases

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming接受socket数据实现所有批次的单词次数累加
  *
  * @author yangwei
  */
object Case04_UpdateStateByKeyWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // 1. 创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    // 2. 创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // 3. 设置checkpoint目录
    ssc.checkpoint("hdfs://node01:8020/checkpoint")

    // 4. 接收Socket数据
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)

    // 5. 对数据进行处理
    val result: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_, 1))
      .updateStateByKey(updateFunc)

    // 6. 打印结果
    result.print()

    // 7. 开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * @param currentValues  表示在当前批次每个单词出现的所有的1 (hadoop,1)、(hadoop,1)、...、(hadoop,1)
    * @param historyValues  表示在之前所有批次中每个单词出现的总次数 (hadoop,100)
    */
  def updateFunc(currentValues: Seq[Int], historyValues: Option[Int]): Option[Int] = {
    val newValue: Int = currentValues.sum + historyValues.getOrElse(0)
    Some(newValue)
  }
}
