package com.yw.flink.example.scalacases.case00

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Flink 流处理 Scala WordCount
  *
  * @author yangwei
  */
object Case02_StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2. 导入隐式转换
    import org.apache.flink.streaming.api.scala._
    // 3. 读取数据文件
    val linesDs: DataStream[String] = env.readTextFile(".data/words.txt")
    // 4. 切分单词，对数据进行计数、分组、聚合统计
    linesDs.flatMap(line => line.split(" "))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
      .print()
    // 5. 执行execute方法触发执行
    env.execute()
  }
}
