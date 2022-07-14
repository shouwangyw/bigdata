package com.yw.spark.example.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object WordCountLocalV2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(WordCountLocalV2.getClass.getSimpleName)
      .setMaster("local[*]")

    val sc: SparkContext = new SparkContext(sparkConf)

    val tuples: Array[(String, Int)]= sc.textFile(ClassLoader.getSystemResource("word.csv").getPath)
      .flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect()

    tuples.foreach(println)
    sc.stop()
  }
}
