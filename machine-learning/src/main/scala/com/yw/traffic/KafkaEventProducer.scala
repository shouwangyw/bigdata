package com.yw.traffic

import net.sf.json.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties

/**
 * 向kafka car_events中生产数据
 */
object KafkaEventProducer {
  def main(args: Array[String]): Unit = {
    val topic = "car_events"
    val props = new Properties()
    //生产数据的时候，直连kafka   消费数据:zookeeper(用户消费数据的偏移量)
    props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val sparkConf = new SparkConf().setAppName("traffic data").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    val records: Array[Array[String]] = sc.textFile("machine-learning/data/carFlow_all_column_test.txt")
      .filter(!_.startsWith(";"))
      .filter(one => {
        !"00000000".equals(one.split(",")(2))
      })
      .filter(_.split(",")(6).toInt != 255)
      .filter(_.split(",")(6).toInt != 0)
      .map(_.split(",")).collect()

    for (i <- 1 to 1000) {
      for (record <- records) {
        // prepare event data
        val event = new JSONObject()
        event.put("camera_id", record(0))
        event.put("car_id", record(2))
        event.put("event_time", record(4))
        event.put("speed", record(6))
        //n车道   12车道
        event.put("road_id", record(13))
        // produce event message
        producer.send(new ProducerRecord[String, String](topic, event.toString))
        Thread.sleep(200)
      }
    }
    sc.stop
  }
}
