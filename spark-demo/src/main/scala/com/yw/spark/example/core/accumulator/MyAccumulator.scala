package com.yw.spark.example.core.accumulator

import org.apache.spark.util.AccumulatorV2

/**
  * 自定义Int累加器
  * @author yangwei
  */
class MyAccumulator extends AccumulatorV2[Int, Int] {
  var sum: Int = 0

  // 判断累加的值是不是空
  override def isZero: Boolean = sum == 0

  // 如何把累加器copy到Executor
  override def copy(): AccumulatorV2[Int, Int] = {
    val acc = new MyAccumulator
    acc.sum = sum
    acc
  }

  // 重置值
  override def reset(): Unit = {
    sum = 0
  }

  // 分区内的累加
  override def add(v: Int): Unit = {
    sum += v
  }

  // 分区间的累加，累加器最终的值
  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    other match {
      case o: MyAccumulator => this.sum += o.sum
      case _ =>
    }
  }

  override def value: Int = this.sum
}
