package com.yw.flink.example.scalacases.case02_transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  *
  * @author yangwei
  */
object Case07_Union {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val ds1: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5))
    val ds2: DataStream[Int] = env.fromCollection(List(5, 4, 8, 9, 10))
    val result: DataStream[Int] = ds1.union(ds2)
    result.print()
    env.execute()
  }
}
