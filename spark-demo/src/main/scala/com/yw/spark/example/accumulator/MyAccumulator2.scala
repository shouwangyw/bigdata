package com.yw.spark.example.accumulator

import org.apache.spark.util.AccumulatorV2

/**
  * 自定义map平均值累加器
  *
  * @author yangwei
  */
class MyAccumulator2 extends AccumulatorV2[Int, Map[String, Double]] {
  var map: Map[String, Double] = Map[String, Double]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[Int, Map[String, Double]] = {
    val acc = new MyAccumulator2
    acc.map ++= map
    acc
  }

  override def reset(): Unit = {
    map = Map[String, Double]()
  }

  override def add(v: Int): Unit = {
    map += "sum" -> (map.getOrElse("sum", 0d) + v)
    map += "count" -> (map.getOrElse("count", 0d) + 1)
  }

  override def merge(other: AccumulatorV2[Int, Map[String, Double]]): Unit = {
    other match {
      case o: MyAccumulator2 =>
        this.map += "sum" -> (map.getOrElse("sum", 0d) + o.map.getOrElse("sum", 0d))
        this.map += "count" -> (map.getOrElse("count", 0d) + o.map.getOrElse("count", 0d))
      case _ =>
    }
  }

  override def value: Map[String, Double] = {
    map += "avg" -> map.getOrElse("sum", 0d) / map.getOrElse("count", 0d)
    map
  }
}
