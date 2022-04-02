package com.yw.flink.example._02_datastream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
  * 测试 connect
  */
object ConnectTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val firstStream = env.fromCollection(Array("hello world", "spark flink"))
    val secondStream = env.fromCollection(Array(1, 2, 3, 4))

    // 连接两个流
    val connectStream = firstStream.connect(secondStream)

    val resultStream = connectStream.flatMap(new CoFlatMapFunction[String, Int, String] {
      override def flatMap1(in: String, out: Collector[String]): Unit = {
        out.collect(in.toUpperCase())
      }

      override def flatMap2(in: Int, out: Collector[String]): Unit = {
        out.collect(in * 2 + "")
      }
    })

    resultStream.print()

    env.execute()
  }
}
