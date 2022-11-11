//package com.yw.spark.example.streaming.cases
//
//import kafka.serializer.StringDecoder
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
//  * sparkStreaming使用kafka 0.8API基于Direct直连来接受消息
//  * spark direct API接收kafka消息，从而不需要经过zookeeper，直接从broker上获取信息。
//  *
//  * @author yangwei
//  */
//object Case13_KafkaDirect08 {
//  private val kafkaCluster = "node01:9092,node02:9092,node03:9092"
//  private val groupId = "KafkaDirect08"
//  private val topics = Set("test")
//
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
//    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
//      // 开启 WAL 机制
//      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
//    val ssc = new StreamingContext(sparkConf, Seconds(2))
//
//    // 接收kafka数据
//    val kafkaParams = Map(
//      "metadata.broker.list" -> kafkaCluster,
//      "group.id" -> groupId
//    )
//    // 使用direct直连的方式接收数据
//    val kafkaDstream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//    // 获取kafka的topic数据
//    val data: DStream[String] = kafkaDstream.map(_._2)
//    // 单词计算
//    val result: DStream[(String, Int)] = data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
//
//    result.print()
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
