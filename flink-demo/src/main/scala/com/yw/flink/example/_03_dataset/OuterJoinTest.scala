package com.yw.flink.example._03_dataset

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ArrayBuffer

/**
  * 测试 leftOuterJoin、rightOuterJoin
  */
object OuterJoinTest {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val buffer1 = ArrayBuffer((1, "zhangsan"), (2, "lisi"), (3, "wangwu"), (4, "Tom"))
    val buffer2 = ArrayBuffer((1, 23), (2, 14), (3, 35), (5, 50))

    val dataSet1 = env.fromCollection(buffer1)
    val dataSet2 = env.fromCollection(buffer2)

    // 左外连接
    val leftOuterJoin = dataSet1.leftOuterJoin(dataSet2).where(0).equalTo(0)
    val leftResult = leftOuterJoin.apply(new JoinFunction[(Int, String), (Int, Int), (Int, String, Int)] {
      override def join(left: (Int, String), right: (Int, Int)): (Int, String, Int) = {
        val result = if (right == null) {
          Tuple3[Int, String, Int](left._1, left._2, -1)
        } else {
          Tuple3[Int, String, Int](left._1, left._2, right._2)
        }
        result
      }
    })
    leftResult.print()

    // 右外连接
    val rightOuterJoin = dataSet1.rightOuterJoin(dataSet2).where(0).equalTo(0)
    val rightResult = rightOuterJoin.apply(new JoinFunction[(Int, String), (Int, Int), (Int, Int, String)] {
      override def join(left: (Int, String), right: (Int, Int)): (Int, Int, String) = {
        val result = if (left == null) {
          Tuple3[Int, Int, String](right._1, right._2, "null")
        } else {
          Tuple3[Int, Int, String](right._1, right._2, left._2)
        }
        result
      }
    })

    rightResult.print()
  }
}
