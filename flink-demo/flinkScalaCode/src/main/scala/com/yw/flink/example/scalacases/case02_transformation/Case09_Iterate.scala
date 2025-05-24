package com.yw.flink.example.scalacases.case02_transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  *
  * @author yangwei
  */
object Case09_Iterate {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)
    val ds2: DataStream[Int] = ds.map(_.toInt)

    val result: DataStream[Int] = ds2.iterate(iteration => {
      //定义迭代体
      val minusOne: DataStream[Int] = iteration.map(v => {
        println("迭代体中进入的数据为 = " + v)
        v - 1
      })

      //定义迭代条件
      val cn: DataStream[Int] = minusOne.filter(_ > 0)

      //定义哪些数据最后输出
      val end: DataStream[Int] = minusOne.filter(_ != 0)

      (cn, end)
    })

    result.print()
    env.execute()

  }

}
