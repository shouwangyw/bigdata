package com.yw.spark.example.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object T06_foldByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd1: RDD[String] = sc.parallelize(Array("hello", "hadoop", "hello", "spark"), 1)
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    // 方式一：
    rdd2.foldByKey(0)((x, y) => {
      x + y
    }).foreach(x => print(x + "\t"))
    println
    println("------------------------------------")

    // 方式二：简写
    val result: RDD[(String, Int)] = rdd2.foldByKey(0)(_ + _)
    result.foreach(x => print(x + "\t")) // (spark,1)	(hadoop,1)	(hello,2)
    println
    println("------------------------------------")

    sc.stop()
  }
}
