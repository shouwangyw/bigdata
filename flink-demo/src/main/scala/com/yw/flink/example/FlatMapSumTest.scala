package com.yw.flink.example

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * 测试 flatMap、keyBy、sum
  */
object FlatMapSumTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceStream = env.socketTextStream("node01", 9999)

    val resultStream = sourceStream
          .flatMap(x => x.split(" "))
          .map(x => (x, 1))
          .keyBy(0)
          .sum(1)

    resultStream.print()

    env.execute()
  }
}
