package com.yw.spark.example.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object T08_sortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array("he", "hadoop", "llo", "spark"), 1)
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    rdd2.sortByKey().foreach(x => print(x + "\t")) // 默认升序
    println
    println("------------------------------------")
    rdd2.sortByKey(false).foreach(x => print(x + "\t"))
    println
    println("------------------------------------")

    sc.stop()
  }
}
