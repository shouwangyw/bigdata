package com.yw.flink.example

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
//导入隐式转换的包
import org.apache.flink.api.scala._

/**
  * 通过scala开发 flink 流处理作业
  */
object WordCountStreamScala {
  def main(args: Array[String]): Unit = {
    // 1. 构建流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. 从socket获取数据
    val sourceStream: DataStream[String] = env.socketTextStream("node01", 9999)

    // 3. 对数据进行处理
    val result: DataStream[(String, Int)] = sourceStream
            .flatMap(x => x.split(" ")) // 按照空格切分
            .map(x => (x, 1)) // 每个单词计为1
            .keyBy(x => x._1) // 按照单词进行分组
            .sum(1) // 按照下标为1累加相同单词出现的次数

    // 4. 打印输出, sink
    result.print()

    // 5. 开启任务
    env.execute("WordCountStreamScala")
  }
}
