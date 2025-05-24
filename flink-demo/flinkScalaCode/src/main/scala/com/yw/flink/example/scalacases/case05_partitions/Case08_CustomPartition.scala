package com.yw.flink.example.scalacases.case05_partitions

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Flink 自定义分区策略
  */
object Case08_CustomPartition {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)
    val ds2: DataStream[(String, Int)] = ds.map(one => {
      val split: Array[String] = one.split(",")
      (split(0), split(1).toInt)
    })

    val result: DataStream[(String, Int)] = ds2.partitionCustom(new Partitioner[String] {
      override def partition(k: String, i: Int): Int = k.hashCode % i
    }, tp => {
      tp._1
    })

    result.print()
    env.execute()

  }

}
