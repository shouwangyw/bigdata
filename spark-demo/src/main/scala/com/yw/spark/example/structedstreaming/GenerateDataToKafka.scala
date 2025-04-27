package com.yw.spark.example.structedstreaming

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * 向Kafka 中生产 json 数据
  */
object GenerateDataToKafka {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "node1:9092,node2:9092,node3:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](props)

    val in: InputStream = this.getClass.getClassLoader.getResourceAsStream("client_play_songinfo")
    val br: BufferedReader = new BufferedReader(new InputStreamReader(in,"UTF-8"))

    var counter = 0

    var line: String = br.readLine
    while(line!=null){
      println("line = "+line)
      producer.send(new ProducerRecord[String, String]("playsong-topic", line))
      line = br.readLine()

      counter +=1
      if(0 == counter%100){
        counter = 0
        Thread.sleep(5000)
      }
    }
    producer.close()
  }
}
