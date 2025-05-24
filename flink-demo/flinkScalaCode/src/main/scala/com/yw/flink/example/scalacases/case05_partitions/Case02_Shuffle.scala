package com.yw.flink.example.scalacases.case05_partitions

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  *
  * @author yangwei
  */
object Case02_Shuffle {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)
    ds.shuffle.print().setParallelism(3)
    env.execute()
  }

}
