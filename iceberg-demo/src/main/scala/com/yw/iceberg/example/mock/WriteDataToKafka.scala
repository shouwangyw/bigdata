package com.yw.iceberg.example.mock

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import scala.util.Random

/**
 * 向kafka中写入数据
 *
 * @author yangwei
 */
object WriteDataToKafka {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    var counter = 0
    var keyFlag = 0
    while (true) {
      counter += 1
      keyFlag += 1
      val content: String = userlogs()
      producer.send(new ProducerRecord[String, String]("test-iceberg-topic", content))
      if (0 == counter % 100) {
        counter = 0
        Thread.sleep(5000)
      }
    }
    producer.close()
  }

  def userlogs() = {
    val userLogBuffer = new StringBuffer("")
    val timestamp = new Date().getTime
    var userId = 0L
    var pageId = 0L

    // 随机生成用户ID和页面ID
    userId = Random.nextInt(2000)
    pageId = Random.nextInt(2000)

    // 随机生成Channel
    val channelNames = Array[String]("Spark", "Scala", "Kafka", "Flink", "Hadoop", "Storm", "Hive", "Impala", "HBase", "ML")
    val channel = channelNames(Random.nextInt(10))

    val actionName = Array[String]("View", "Register")
    // 随机生成action行为
    val action = actionName(Random.nextInt(2))

    val dateToday = new SimpleDateFormat("yyyy-MM-dd").format(new Date())

    userLogBuffer.append(dateToday).append("\t")
      .append(timestamp).append("\t")
      .append(userId).append("\t")
      .append(pageId).append("\t")
      .append(channel).append("\t")
      .append(action)

    System.out.println(userLogBuffer.toString)
    userLogBuffer.toString
  }
}
