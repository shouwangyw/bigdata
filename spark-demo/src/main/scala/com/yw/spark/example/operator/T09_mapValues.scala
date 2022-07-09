package com.yw.spark.example.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object T09_mapValues {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array("hello", "hadoop", "hello", "spark"), 1)
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    rdd2.mapValues(x => x + 1).foreach(x => print(x + "\t")) // (hello,2)	(hadoop,2)	(hello,2)	(spark,2)

    sc.stop()
  }
}
