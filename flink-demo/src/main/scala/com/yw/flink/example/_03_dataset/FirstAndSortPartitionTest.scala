package com.yw.flink.example._03_dataset

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

import scala.collection.mutable.ArrayBuffer

/**
  * 测试 first、sortPartition
  */
object FirstAndSortPartitionTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val buffer1 = ArrayBuffer((1, "zhangsan", 13), (2, "lisi", 24), (3, "wangwu", 5), (4, "Tom", 66))

    val dataSet = env.fromCollection(buffer1)

    // 获取前三个元素
    dataSet.first(3).print()
    println("------------------")

    dataSet.groupBy(0) // 按照第一个字段进行分组
      .sortGroup(2, Order.DESCENDING) // 按照第三个字段进行排序
      .first(1) // 获取每组的前一个元素
      .print()
    println("------------------")

    dataSet.sortPartition(0, Order.DESCENDING).sortPartition(2, Order.ASCENDING).print()
  }
}
