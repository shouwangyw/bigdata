package com.yw.flink.example

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
  * Flink Scala 类型推断
  */
object WCTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    val ds: DataStream[String] = env.socketTextStream("nc_server", 9999)
    ds.flatMap(one => {
      val arr: Array[String] = one.split(" ")
      val listBuffer = new ListBuffer[Tuple2[String, Int]]
      for (word <- arr) {
        listBuffer.append((word, 1))
      }
      listBuffer
    }).print()

    env.execute()
  }

}
