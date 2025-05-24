package com.yw.flink.example.scalacases.case01_source

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTuple2TypeInformation, createTypeInformation}
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
  * Scala -Flink 读取Kafka数据 - 有key 有value
  *
  * kafka-console-producer.sh --bootstrap-server node1:9092,node2:9092,node3:9092 --topic testtopic --property parse.key=true --property key.separator='-'
  */
object Case03_KafkaSourceWithKey {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val kafkaSource: KafkaSource[(String, String)] = KafkaSource.builder[(String, String)]()
      .setBootstrapServers("node1:9092,node2:9092,node3:9092")
      .setTopics("testtopic")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setDeserializer(new KafkaRecordDeserializationSchema[(String, String)] {
        override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]],
                                 collector: Collector[(String, String)]): Unit = {
          var key: String = null
          var value: String = null
          if (consumerRecord.key() != null) {
            key = new String(consumerRecord.key(), "UTF-8")
          }
          if (consumerRecord.value() != null) {
            value = new String(consumerRecord.value(), "UTF-8")
          }

          collector.collect((key, value))
        }

        //指定返回数据类型
        override def getProducedType: TypeInformation[(String, String)] = {
          createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[String])
        }
      }).build()

    val result: DataStream[(String, String)] = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-key-value")
    result.print()
    env.execute()

  }

}
