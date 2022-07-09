package com.yw.spark.example.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object T10_join {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("a", 10), ("b", 10), ("a", 20), ("d", 10)))
    val rdd2: RDD[(String, Int)] = sc.parallelize(Array(("a", 30), ("b", 20), ("c", 20)))

//    // 内连接
//    rdd1.join(rdd2).foreach(x => print(x + "\t")) // (a,(10,30))	(b,(10,20))	(a,(20,30))
//    println("------------------------------------")
//
    // 左连接
    rdd1.leftOuterJoin(rdd2).foreach(x => print(x + "\t")) // (d,(10,None))	(a,(10,Some(30)))	(b,(10,Some(20)))	(a,(20,Some(30)))
    println("------------------------------------")

//    // 右连接
//    rdd1.rightOuterJoin(rdd2).foreach(x => print(x + "\t")) // (c,(None,20))	(b,(Some(10),20)) (a,(Some(10),30))	(a,(Some(20),30))
//    println("------------------------------------")

//    // 全连接
//    rdd1.fullOuterJoin(rdd2).foreach(x => print(x + "\t")) // (d,(Some(10),None))	(b,(Some(10),Some(20)))	(c,(None,Some(20)))	(a,(Some(10),Some(30)))	(a,(Some(20),Some(30)))
//    println("------------------------------------")

    sc.stop()
  }
}
