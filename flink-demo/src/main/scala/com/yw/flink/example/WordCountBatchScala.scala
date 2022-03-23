package com.yw.flink.example

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * scala开发flink的批处理程序
  */
object WordCountBatchScala {
  def main(args: Array[String]): Unit = {
    // 1. 构建Flink的批处理环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 2. 读取数据文件
    val fileDataSet: DataSet[String] = env.readTextFile("words.txt")

    // 3. 对数据进行处理
    val resultDataSet: DataSet[(String, Int)] = fileDataSet
            .flatMap(x => x.split(" "))
            .map(x => (x, 1))
            .groupBy(0)
            .sum(1)

    // 4. 打印结果
    resultDataSet.print()

    // 5. 保存结果到文件
    resultDataSet.writeAsText("output")
    env.execute("FlinkFileCount")
  }
}
