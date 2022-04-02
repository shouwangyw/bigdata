package com.yw.flink.example._03_dataset

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

import scala.collection.mutable.ArrayBuffer

/**
  * 测试 join
  */
object JoinTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val buffer1 = ArrayBuffer((1, "zhangsan"), (2, "lisi"), (3, "wangwu"))
    val buffer2 = ArrayBuffer((1, 23), (2, 14), (3, 35))

    val dataSet1 = env.fromCollection(buffer1)
    val dataSet2 = env.fromCollection(buffer2)

    val result = dataSet1.join(dataSet2).where(0)
            .equalTo(0).map(x => (x._1._1, x._1._2, x._2._2))

    result.print()
  }
}
