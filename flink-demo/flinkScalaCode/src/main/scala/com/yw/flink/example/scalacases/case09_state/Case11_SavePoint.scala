package com.yw.flink.example.scalacases.case09_state

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Flink SavePoint 保存点测试
 */
object Case11_SavePoint {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    env.socketTextStream("nc_server",9999).uid("socket-source")
      .flatMap(_.split(",")).uid("flatMap")
      .map((_,1)).uid("map")
      .keyBy(_._1)
      .sum(1).uid("sum")
      .print().uid("print")
    env.execute()

  }
}
