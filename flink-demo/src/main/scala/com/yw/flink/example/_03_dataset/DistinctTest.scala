package com.yw.flink.example._03_dataset

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

import scala.collection.mutable.ArrayBuffer

/**
  * 测试 distinct
  */
object DistinctTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val buffer = ArrayBuffer[String]()
    buffer.+=("Hello World")
    buffer.+=("Hello World")
    buffer.+=("Hello World")
    buffer.+=("Hello World")

    val dataSet = env.fromCollection(buffer)

    val result = dataSet.flatMap(x => x.split(" ")).distinct()

    result.print()
  }
}
