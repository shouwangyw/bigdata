//package com.yw.spark.example.streaming.cases
//
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.serialization.StringDeserializer
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
//  * sparkStreaming使用kafka 1.0API基于Direct直连来接受消息
//  *
//  * @author yangwei
//  */
//object Case14_KafkaDirect10 {
//  private val kafkaCluster = "node01:9092,node02:9092,node03:9092"
//  private val groupId = "KafkaDirect10"
//  private val topics = Set("test")
//
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    // 1. 创建StreamingContext对象
//    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
//    val ssc = new StreamingContext(sparkConf, Seconds(2))
//
//    // 2. 使用Direct接收kafka数据
//    val kafkaParams = Map(
//      "bootstrap.servers" -> kafkaCluster,
//      "group.id" -> groupId,
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "enable.auto.commit" -> "false"
//    )
//    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      // 数据本地性策略
//      LocationStrategies.PreferConsistent,
//      // 指定要订阅的topic
//      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
//    )
//
//    // 3. 对数据进行处理
//    // 注意：如果你想获取到消息消费的偏移，这里需要拿到最开始的这个DStream进行操作
//    // 如果你对该DStream进行了其他的转换之后，生成了新的DStream，新的DStream不再保存对应的消息的偏移量
//    kafkaDStream.foreachRDD(rdd => {
//      // 获取消息内容
//      val dataRdd: RDD[String] = rdd.map(_.value())
//      // 打印
//      dataRdd.foreach(line => println(line))
//
//      // 4. 提交偏移量，将偏移量信息添加到kafka中
//      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
//    })
//
//    // 5. 开启流式计算
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
