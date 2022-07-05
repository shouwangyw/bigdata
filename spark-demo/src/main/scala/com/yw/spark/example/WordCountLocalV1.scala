package com.yw.spark.example

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object WordCountLocalV1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName(WordCountLocalV1.getClass.getSimpleName)
      .setMaster("local[*]")

    val sc: SparkContext = new SparkContext(sparkConf)

    val tuples: Array[(String, Int)]= sc.textFile(ClassLoader.getSystemResource("word.csv").getPath)
      .flatMap(line => line.split(","))
      .map(word => (word, 1))
      .reduceByKey((x, y) => x + y)
      .collect()

    tuples.foreach(println)
    sc.stop()
  }
}
