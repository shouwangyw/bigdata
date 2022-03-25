package com.yw.flink.example

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * 基于数组或者集合构建DataStream
  */
object StreamSourceFromCollection {
  def main(args: Array[String]): Unit = {
    // 1. 获取流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. 准备数据源--数组
    val array = Array("hello world","world spark","flink test","spark hive","test")
    val fromArray: DataStream[String] = env.fromCollection(array)

    // 3. 数据处理
    val resultDataStream: DataStream[(String, Int)] = fromArray
                .flatMap(x => x.split(" "))
                .map(x =>(x,1))
                .keyBy(0)
                .sum(1)
    // 4. 打印
    resultDataStream.print()

    // 5. 启动
    env.execute()
  }
}
