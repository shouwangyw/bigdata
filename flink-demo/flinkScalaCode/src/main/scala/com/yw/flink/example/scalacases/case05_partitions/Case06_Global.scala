package com.yw.flink.example.scalacases.case05_partitions

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Flink 中的Global 分区策略
  */
object Case06_Global {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val ds1: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    ds1.global.print()
    env.execute()

  }

}
