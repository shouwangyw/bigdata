package com.yw.flink.example._03_dataset

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

import scala.collection.mutable.ArrayBuffer

/**
  * 测试 cross
  */
object CrossTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val buffer1 = ArrayBuffer((1, "zhangsan"), (2, "lisi"), (3, "wangwu"), (4, "Tom"))
    val buffer2 = ArrayBuffer((1, 23), (2, 14), (3, 35), (5, 50))

    val dataSet1 = env.fromCollection(buffer1)
    val dataSet2 = env.fromCollection(buffer2)

    // cross笛卡尔积
    val result = dataSet1.cross(dataSet2)

    result.print()
  }
}
