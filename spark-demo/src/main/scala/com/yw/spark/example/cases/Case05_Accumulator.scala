package com.yw.spark.example.cases

import com.yw.spark.example.accumulator.MyAccumulator
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object Case05_Accumulator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

//    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5), 2)
//    var a = 1
//    rdd1.foreach(x => {
//      a += 1
//      println("rdd: " + a)
//    })
//    println("----------------------")
//    println("main: " + a)

//    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5), 2)
//    val acc: LongAccumulator = sc.longAccumulator("acc")
//    rdd1.foreach(x => {
//      acc.add(1)
//      println("rdd: " + acc.value)
//    })
//    println("----------------------")
//    println("main: " + acc.count)

    val rdd1: RDD[Int] = sc.parallelize(Array(1, 2, 3, 4, 5), 2)
    val acc = new MyAccumulator
    // 注册累加器
    sc.register(acc)

    rdd1.foreach(x => {
      acc.add(1)
      println("rdd: " + acc.value)
    })
    println("----------------------")
    println("main: " + acc.value)
  }
}
