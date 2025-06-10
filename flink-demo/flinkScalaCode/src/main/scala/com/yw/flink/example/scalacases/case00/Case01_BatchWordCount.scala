package com.yw.flink.example.scalacases.case00

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Flink 批处理 Scala WordCount
  *
  * @author yangwei
  */
object Case01_BatchWordCount {
  def main(args: Array[String]): Unit = {
    // 1. 准备环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    // 2. 导入隐式转换
    import org.apache.flink.api.scala._
    // 3. 读取数据文件
    val linesDs: DataSet[String] = env.readTextFile(".data/words.txt")
    // 4. 切分单词
    val wordsDs: DataSet[String] = linesDs.flatMap(line => line.split(" "))
    // 5. 对数据进行计数、分组、聚合统计
    wordsDs.map(word => {
      (word, 1)
    }).groupBy(0)
      .sum(1)
      .print()
  }
}
