package com.yw.spark.example.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object T11_cogroup {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("a", 10), ("b", 10), ("a", 20), ("d", 10)))
    val rdd2: RDD[(String, Int)] = sc.parallelize(Array(("a", 30), ("b", 20), ("c", 20)))

    rdd1.cogroup(rdd2).foreach(println)

    sc.stop()
  }
}
