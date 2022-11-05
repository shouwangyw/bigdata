package com.yw.flume

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark Streaming 整合flume 拉模式Poll
  * @author yangwei
  */
object SparkStreamingPollFlume {
  def main(args: Array[String]): Unit = {
    // 1. 创建SparkConf对象，注意这里至少给两个线程，一个线程没办法执行
    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    // 2. 创建SparContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    // 3. 创建StreamingContext对象
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./flume")

    // 4. 通过FlumeUtils调用createPollingStream方法获取flume中的数据
    val pollingStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, "node02", 8888)

    // 5. 获取flume中event的body
    val data: DStream[String] = pollingStream.map(x => new String(x.event.getBody.array()))

    // 6. 切分每一行，每个单词记为1
    val wordAndOne: DStream[(String, Int)] = data.flatMap(x => x.split(" ")).map((_, 1))

    // 7. 相同单词出现的次数累加
    val result: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc)

    // 8. 打印结果
    result.print()

    // 9. 开启流式计算
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
