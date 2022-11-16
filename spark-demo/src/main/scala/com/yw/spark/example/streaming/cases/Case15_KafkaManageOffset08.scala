//package com.yw.spark.example.streaming.cases
//
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
//import org.I0Itec.zkclient.ZkClient
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
//  * sparkStreaming使用kafka 0.8API基于Direct直连来接受消息
//  * 手动将偏移量数据保存到ZK中
//  *
//  * @author yangwei
//  */
//object Case15_KafkaManageOffset08 {
//  private val kafkaCluster = "node01:9092,node02:9092,node03:9092"
//  private val zkQuorum = "192.168.254.120:2181"
//  private val groupId = "consumer-manager"
//  private val topic = "wordcount"
//  private val topics = Set(topic)
//
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
//    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
//      // 开启 WAL 机制
//      .set("spark.streaming.receiver.writeAheadLog.enable", "true")
//    val ssc = new StreamingContext(sparkConf, Seconds(2))
//
//    // 创建一个 ZKGroupTopicDirs 对象，就是用来指定在zk中的存储目录，用来保存数据偏移量
//    val topicDirs = new ZKGroupTopicDirs(groupId, topic)
//    // 获取 ZK 中的路径 "/consumers/consumer-manager/offsets/wordcount"
//    val zkTopicPath = topicDirs.consumerOffsetDir
//    // 构造一个ZK的客户端，用来读写偏移量数据
//    val zkClient = new ZkClient(zkQuorum)
//    // 准备kafka的参数
//    val kafkaParams = Map(
//      "metadata.broker.list" -> kafkaCluster,
//      "group.id" -> groupId,
//      "enable.auto.commit" -> "false"
//    )
//    // 定义kafkaStream流
//    var kafkaStream: InputDStream[(String, String)] = null
//    // 获取指定的zk节点的子节点个数
//    val childrenNum = zkClient.countChildren(zkTopicPath)
//    // 判断是否保存过数据: 根据子节点的数量是否为0
//    if (childrenNum > 0) {
//      var fromOffsets: Map[TopicAndPartition, Long] = Map()
//      for (i <- 0 until childrenNum) {
//        // 获取子节点
//        val partitionOffset: String = zkClient.readData[String](s"$zkTopicPath/$i")
//        val tp = TopicAndPartition(topic, i)
//        // 获取数据偏移量: 将不同分区内的数据偏移量保存到map集合中
//        fromOffsets += (tp -> partitionOffset.toLong)
//      }
//      // 泛型中 key, kafka中的key   value：hello tom hello jerry
//      // 创建函数 解析数据 转换为（topic_name, message）的元组
//      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
//      // 利用底层的API创建DStream: 采用直连的方式(若之前已经消费了，则从指定的位置消费)
//      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
//    } else {
//      // 利用底层的API创建DStream 采用直连的方式(若之前没有消费，则这是第一次读取数据)
//      // zk中没有子节点数据，就是第一次读取数据，直接创建直连对象
//      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//    }
//    // 直接操作kafkaStream
//    // 依次迭代DStream中的kafkaRDD, 只有kafkaRDD才可以强转为HasOffsetRanges, 从中获取数据偏移量信息
//    // 之后是操作的RDD, 不能够直接操作DStream, 因为调用Transformation方法之后就不是kafkaRDD了获取不了偏移量信息
//    kafkaStream.foreachRDD(kafkaRDD => {
//      // 强转为HasOffsetRanges, 获取offset偏移量数据
//      val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
//      // 获取数据
//      val lines: RDD[String] = kafkaRDD.map(_._2)
//      // 接下来就是对RDD进行操作 触发action
//      lines.foreachPartition(partition => partition.foreach(x => println(x)))
//      // 手动提交偏移量到zk集群上
//      for (o <- offsetRanges) {
//        // 拼接zk路径
//        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
//        // 将 partition 的偏移量数据 offset 保存到zookeeper中
//        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
//      }
//    })
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
