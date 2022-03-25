package com.yw.flink.example

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * 本地调试并行度
  */
object ParallelismTest {
  def main(args: Array[String]): Unit = {
    // 1. 使用 createLocalEnvironmentWithWebUI 方法，构建本地流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    // 设置环境级别 并行度
    env.setParallelism(4)

    // 2. 接收socket数据
    val sourceStream: DataStream[String] = env
      .socketTextStream("node01", 9999) // source task

    // 3. 数据处理
    sourceStream.flatMap(x => x.split(" ")).setParallelism(3) // flatMap task
      .map(x => (x, 1)) // map task
      .keyBy(0)
      .sum(1)  // sum task
      .print()  // print task

    // 4. 启动
    env.execute()
  }
}
