package com.yw.flink.example.scalacases.case02_transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  *
  * @author yangwei
  */
object Case03_Filter {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val ds: DataStream[Int] = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8))
    ds.filter(_ % 2 == 0).print()
    env.execute()

  }

}
