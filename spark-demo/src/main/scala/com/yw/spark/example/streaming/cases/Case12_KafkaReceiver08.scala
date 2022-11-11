//package com.yw.spark.example.streaming.cases
//
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
//  * sparkStreaming使用kafka 0.8API基于recevier来接受消息
//  *
//  * @author yangwei
//  */
//object Case12_KafkaReceiver08 {
//  private val zkQuorum = "192.168.254.120:2181"
//  private val groupId = "KafkaReceiver08"
//  private val topics = Map("test" -> 1)
//
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
//    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
//      // 开启 WAL 机制
//      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
//    val ssc = new StreamingContext(sparkConf, Seconds(2))
//
//    // 设置checkpoint，将接收到的数据持久化写入到HDFS
//    ssc.checkpoint("hdfs://node01:8020/wal")
//
//    // 接收kafka数据
//    val receiverDstream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)
//    // 获取kafka的topic数据
//    val data: DStream[String] = receiverDstream.map(_._2)
//    // 单词计算
//    val result: DStream[(String, Int)] = data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
//
//    result.print()
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
