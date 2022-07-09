package com.yw.spark.example.cases

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object Case06_Cache {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array("a A", "b B", "c C"))
    val rdd2: RDD[String] = rdd1.flatMap(x => {
      println("执行flatMap操作")
      x.split("")
    })
    val rdd3: RDD[(String, Int)] = rdd2.map((_, 1))

    //    /* 持久化到内存*/
    //    rdd3.cache()

    /**
      * 持久化到磁盘
      * DISK_ONLY：持久化到磁盘
      * DISK_ONLY_2：持久化到磁盘并且存一个副本（2个文件）
      * MEMORY_ONLY：持久化到内存
      * MEMORY_ONLY_2：持久化到内存并且存一个副本（2个文件）
      * MEMORY_ONLY_SER：持久化到内存，并且序列化
      * MEMORY_ONLY_SER_2：持久化到内存，并且序列化，还要存一个副本（2个文件）
      * MEMORY_AND_DISK：持久化到内存和磁盘
      * MEMORY_AND_DISK_2：持久化到内存和磁盘并且存一个副本（2个文件）
      * MEMORY_AND_DISK_SER：持久化到内存和磁盘，并且序列化
      * MEMORY_AND_DISK_SER_2：持久化到内存和磁盘，并且序列化，还要存一个副本（2个文件）
      * OFF_HEAP：持久化在堆外内存中，Spark自己管理的内存
      **/
    rdd3.persist(StorageLevel.DISK_ONLY) // 持久化到磁盘

    // action 算子
    rdd3.collect.foreach(x => print(x + "\t"))
    println("-----------")
    //输出语句println("执行flatMap操作")不会再次执行
    rdd3.collect.foreach(x => print(x + "\t"))
  }
}
