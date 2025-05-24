package com.yw.flink.example.scalacases.case02_transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  *
  * @author yangwei
  */
object Case02_FlatMap {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val ds: DataStream[String] = env.fromCollection(List("1,2", "3,4", "5,6", "7,8"))
    val result: DataStream[String] = ds.flatMap(_.split(","))
    result.print()
    env.execute()
  }
}
