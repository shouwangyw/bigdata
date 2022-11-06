package com.yw.spark.example.streaming.cases

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{MapWithStateDStream, ReceiverInputDStream}

/**
  * Spark Streaming接受socket数据实现所有批次的单词次数累加
  *
  * @author yangwei
  */
object Case05_MapWithStateWordCount {
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

    val initRDD: RDD[(String, Int)] = ssc.sparkContext.parallelize(List(("hadoop", 10), ("spark", 20)))
    val stateSpec = StateSpec.function((time: Time, key: String, currentValue: Option[Int], historyState: State[Int]) => {
      val sum: Int = currentValue.getOrElse(0) + historyState.getOption().getOrElse(0)
      val output = (key, sum)
      if (!historyState.isTimingOut()) {
        historyState.update(sum)
      }
      Some(output)
    }).initialState(initRDD).timeout(Durations.seconds(5))

    // 5. 对数据进行处理
    val result: MapWithStateDStream[String, Int, Int, (String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_, 1))
      .mapWithState(stateSpec)

    // 6. 打印结果
    result.stateSnapshots().print()

    // 7. 开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
