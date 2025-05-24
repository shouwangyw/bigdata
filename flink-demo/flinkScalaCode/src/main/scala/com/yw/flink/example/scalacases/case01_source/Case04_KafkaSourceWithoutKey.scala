package com.yw.flink.example.scalacases.case01_source

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Scala 版本 Flink读取Kafka数据 - 只读取value
  */
object Case04_KafkaSourceWithoutKey {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    val kafkaSource: KafkaSource[String] = KafkaSource.builder[String]()
      .setBootstrapServers("node1:9092,node2:9092,node3:9092")
      .setTopics("testtopic")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val result: DataStream[String] = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
    result.print()
    env.execute()
  }
}
