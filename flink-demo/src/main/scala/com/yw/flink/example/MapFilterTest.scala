package com.yw.flink.example

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import  org.apache.flink.api.scala._

/**
  * 测试 map、filter
  */
object MapFilterTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceStream = env.fromElements(1, 2, 3, 4, 5, 6)

    val mapStream = sourceStream.map(x => x * 10)

    val resultStream = mapStream.filter(x => x % 2 == 0)

    resultStream.print()

    env.execute()
  }
}
