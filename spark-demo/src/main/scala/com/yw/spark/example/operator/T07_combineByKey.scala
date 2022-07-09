package com.yw.spark.example.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object T07_combineByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd1: RDD[String] = sc.parallelize(Array("hello", "hadoop", "hello", "spark"), 1)
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    val result: RDD[(String, Int)] = rdd2.combineByKey(v => v, (c: Int, v: Int) => c + v, (c1: Int, c2: Int) => c1 + c2)
    result.foreach(x => print(x + "\t")) // (spark,1)	(hadoop,1)	(hello,2)
    println
    println("------------------------------------")

    sc.stop()
  }
}
