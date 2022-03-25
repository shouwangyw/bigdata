package com.yw.flink.example

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * 测试 reduce
  */
object ReduceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceStream = env.fromElements(("a", 1), ("a", 2), ("b", 2), ("b", 3), ("c", 2))

    val keyByStream = sourceStream.keyBy(0)

    val resultStream = keyByStream.reduce((t1, t2) => (t1._1, t1._2 + t2._2))

    resultStream.print()

    env.execute()
  }
}
