package com.yw.flink.example.scalacases.case04_sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Flink 写出数据到Kafka，只有value
  */
object Case03_KafkaSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val result: DataStream[String] = env.socketTextStream("node5", 9999)
      .flatMap(_.split(","))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
      .map(tp => {
        tp._1 + "-" + tp._2
      })

    //准备Kafka Sink 对象
    val kafkaSink: KafkaSink[String] = KafkaSink.builder[String]()
      .setBootstrapServers("node1:9092,node2:9092,node3:9092")
      .setProperty("transaction.timeout.ms", 15 * 60 * 1000L + "")
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder()
          .setTopic("flink-topic-2")
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      ).setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
      .build()

    result.sinkTo(kafkaSink)

    env.execute()
  }


}
