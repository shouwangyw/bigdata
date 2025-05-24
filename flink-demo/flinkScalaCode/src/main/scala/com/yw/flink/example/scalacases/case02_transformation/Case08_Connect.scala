package com.yw.flink.example.scalacases.case02_transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  *
  * @author yangwei
  */
object Case08_Connect {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val ds1: DataStream[String] = env.fromCollection(List("a", "b", "c", "d"))
    val ds2: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5))
    val ds3: ConnectedStreams[String, Int] = ds1.connect(ds2)
    val result: DataStream[String] = ds3.map(s => {
      s + "="
    }, i => {
      i + "-"
    })
    result.print()
    env.execute()
  }
}
