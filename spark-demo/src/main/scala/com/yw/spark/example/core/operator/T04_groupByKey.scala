package com.yw.spark.example.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object T04_groupByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd1: RDD[String] = sc.parallelize(Array("hello", "hadoop", "hello", "spark"), 1)
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    val result: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
    result.foreach(x => print(x + "\t")) // (spark,1)	(hadoop,1)	(hello,2)
    println()
    println("------------------------------------")

    result.map(x => (x._1, x._2.size)).foreach(x => print(x + "\t"))

    sc.stop()
  }
}
