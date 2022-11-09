package com.yw.spark.example.streaming.cases

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 通过checkpoint来恢复Driver端
  *
  * @author yangwei
  */
object Case09_DriverHAWordCount {
  val checkpointPath = "hdfs://node01:8020/checkpoint"

  def creatingFunc(): StreamingContext = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint(checkpointPath)

    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01", 9999)
    val result: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunc)

    result.print()

    ssc
  }

  /**
    * @param currentValues  表示在当前批次每个单词出现的所有的1 (hadoop,1)、(hadoop,1)、...、(hadoop,1)
    * @param historyValues  表示在之前所有批次中每个单词出现的总次数 (hadoop,100)
    */
  def updateFunc(currentValues: Seq[Int], historyValues: Option[Int]): Option[Int] = {
    val newValue: Int = currentValues.sum + historyValues.getOrElse(0)
    Some(newValue)
  }

  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpointPath, creatingFunc _)

    ssc.start()
    ssc.awaitTermination()
  }
}
