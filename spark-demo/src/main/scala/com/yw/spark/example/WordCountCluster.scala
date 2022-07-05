package com.yw.spark.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 集群模式运行 简写版本
  * @author yangwei
  */
object WordCountCluster {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(WordCountCluster.getClass.getSimpleName)
//      .setMaster("local[*]")

    val sc: SparkContext = new SparkContext(sparkConf)

    val ret: RDD[(String, Int)]= sc.textFile(args(0))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
//      .collect()

    ret.saveAsTextFile(args(1))

    sc.stop()
  }
}
