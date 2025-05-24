package com.yw.flink.example.scalacases.case02_transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Flink map 测试
  */
object Case01_Map {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val ds: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4))
    ds.map(_ + 1).print()
    env.execute()
  }
}
