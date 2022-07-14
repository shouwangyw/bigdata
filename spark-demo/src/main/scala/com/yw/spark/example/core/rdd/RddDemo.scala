package com.yw.spark.example.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object RddDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(RddDemo.getClass.getSimpleName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    /*
    map算子，一共有多少元数会执行多少次，和分区数无关，可以修改分区数进行测试
     */
    val rdd: RDD[Int] = sc.parallelize(1.to(5), 1)
    val mapRdd: RDD[Int] = rdd.map(x => {
      println("执行") // 一共被执行5次
      x * 2
    })
    val result: Array[Int] = mapRdd.collect()
    result.foreach(x => print(x + "\t"))

//    /*
//    mapPartitions算子，一个分区内处理，有几个分区就执行几次，优于map函数
//    常用于时间转换，数据库连接
//     */
//    val rdd: RDD[Int] = sc.parallelize(1.to(10), 2)
//    val mapRdd: RDD[Int] = rdd.mapPartitions(it => {
//      println("执行") // 分区2次，共打印2次
//      it.map(x => x * 2)
//    })
//    val result: Array[Int] = mapRdd.collect()
//    result.foreach(x => print(x + "\t"))

//    /*
//    mapPartitionsWithIndex算子，一个分区内处理，有几个分区就执行几次
//    返回带有分区号的结果集
//     */
//    val rdd: RDD[Int] = sc.parallelize(1.to(10), 2)
//    val mapRdd: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((idx, it) => {
//      println("执行") // 执行两次
//      it.map((idx, _))
//    })
//
//    val result: Array[(Int, Int)] = mapRdd.collect()
//    result.foreach(x => print(x + "\t"))
  }

//  /**
//    * 读取Object对象文件
//    */
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName(RddDemo.getClass.getSimpleName)
//      .setMaster("local[*]")
//    val sc = new SparkContext(conf)
//
//    val rdd1: RDD[Int] = sc.parallelize(Array(1,2,3,4,5))
//    rdd1.saveAsObjectFile("hdfs://node01:8020/test")
//
//    val rdd2: RDD[Nothing] = sc.objectFile("hdfs://node01:8020/test")
//    rdd2.foreach(println)
//  }

//  /**
//    * 读取json文件
//    */
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName(RddDemo.getClass.getSimpleName)
//      .setMaster("local[*]")
//    val sc = new SparkContext(conf)
//
//    val rdd1: RDD[String] = sc.textFile(this.getClass.getClassLoader.getResource("word.json").getPath)
//    val rdd2: RDD[Option[Any]] = rdd1.map(JSON.parseFull(_))
//    rdd2.foreach(println)
//  }

//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName(RddDemo.getClass.getSimpleName)
//      .setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    /*
//    传入分区数为1，结果顺序打印；传入分区数大于1，结果顺序不定，因为数据被打散在N个分区里
//     */
//    val rdd1: RDD[Int] = sc.parallelize(1.to(10), 1)
//    print("rdd1->:")
//    rdd1.foreach(x => print(x + "\t"))
//    println("")
//
//    val rdd2 = sc.parallelize(List(1, 2, 3, 4, 5), 2)
//    print("rdd2->:")
//    rdd2.foreach(x => print(x + "\t"))
//    println("")
//
//    val rdd3 = sc.parallelize(Array("hadoop", "hive", "spark"), 1)
//    print("rdd3->:")
//    rdd3.foreach(x => print(x + "\t"))
//    println("")
//
//    val rdd4 = sc.makeRDD(List(1, 2, 3, 4), 2)
//    print("rdd4->:")
//    rdd4.foreach(x => print(x + "\t"))
//    println("")
//
//    sc.stop()
//  }
}
