package com.yw.flume

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark Streaming 整合flume 推模式Push
  * @author yangwei
  */
object SparkStreamingPushFlume {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf对象，注意这里至少给两个线程，一个线程没办法执行
    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    // 2. 创建SparContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    // 3. 创建StreamingContext对象
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./flume")

    // 4. 当前应用程序部署的服务器ip地址，跟flume配置文件保持一致
    val flumeStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createStream(ssc, "192.168.0.100", 8888, StorageLevel.MEMORY_AND_DISK)

    // 5. 获取flume中event的body
    val lineStream: DStream[String] = flumeStream.map(x => new String(x.event.getBody.array()))

    // 6. 实现单词汇总
    val result: DStream[(String, Int)] = lineStream.flatMap(x => x.split(" ")).map((_, 1))
      .updateStateByKey(updateFunc)

    // 7. 打印结果
    result.print()

    // 8. 开启流式计算
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
