package com.yw.flink.example

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * 测试 union
  */
object UnionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val firstStream = env.fromCollection(Array("hello spark", "hello flink"))
    val secondStream = env.fromCollection(Array("hadoop spark", "hive flink"))

    // 合并两个流
    val resultStream = firstStream.union(secondStream)

    resultStream.print()

    env.execute()
  }
}
