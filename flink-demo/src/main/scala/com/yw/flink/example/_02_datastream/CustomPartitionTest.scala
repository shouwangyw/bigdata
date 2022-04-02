package com.yw.flink.example._02_datastream

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * 自定义分区策略：实现不同分区的数据发送到不同分区里面去进行处理，将包含hello的字符串发送到一个分区里面去，其他的发送到另外一个分区里面去
  */
object CustomPartitionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceStream = env.fromElements("hello hadoop", "spark flink", "hello flink", "hive hadoop")

    val rePartition = sourceStream.partitionCustom(new MyPartitioner, x => x + "")
    rePartition.map(x => {
      println("数据的key为" + x + ", 线程为" + Thread.currentThread().getId)
      x
    })

    rePartition.print()

    env.execute()
  }
}

/**
  * 定义分区类
  */
class MyPartitioner extends Partitioner[String] {
  override def partition(line: String, num: Int): Int = {
    println("分区数目：" + num)
    if (line.contains("hello")) 0
    else 1
  }
}