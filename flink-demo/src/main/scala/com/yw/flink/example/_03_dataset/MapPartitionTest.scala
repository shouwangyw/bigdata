package com.yw.flink.example._03_dataset

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ArrayBuffer

/**
  * 测试 mapPartition
  */
object MapPartitionTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val buffer = ArrayBuffer[String]()
    buffer.+=("Hello World1")
    buffer.+=("Hello World2")
    buffer.+=("Hello World3")
    buffer.+=("Hello World4")

    val dataSet = env.fromCollection(buffer)

    val result = dataSet.mapPartition(p => p.map(l => l + "!!!"))

    result.print()
  }
}
