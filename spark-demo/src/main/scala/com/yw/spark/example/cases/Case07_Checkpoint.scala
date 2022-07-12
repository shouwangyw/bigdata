package com.yw.spark.example.cases

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object Case07_Checkpoint {
  def main(args: Array[String]): Unit = {
    // 设置当前用户
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 设置checkpoint目录
    sc.setCheckpointDir("hdfs://node01:8020/checkpoint")
    val rdd1: RDD[String] = sc.parallelize(Array("abc"))
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    /**
      * 标记RDD3的checkpoint
      * RDD3会被保存到文件中，并且会切断到父RDD的引用，该持久化操作，必须在job运行之前调用
      * 如果不进行持久化操作，那么在保存到文件的时候需要重新计算
      **/
    val rdd3 = rdd2.cache()
    rdd3.checkpoint()
    rdd3.collect.foreach(x => print(x + "\t"))
    rdd3.collect.foreach(x => print(x + "\t"))
  }
}
