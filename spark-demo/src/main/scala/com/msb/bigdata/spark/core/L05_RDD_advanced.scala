package com.msb.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object L05_RDD_advanced {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName))
    sc.setLogLevel("ERROR")

    val data: RDD[Int] = sc.parallelize(1 to 10,5)
//    println("----------------------")
//    data.sample(true,0.1,222).foreach(println)
//    println("----------------------")
//    data.sample(true,0.1,222).foreach(println)
//    println("----------------------")
//    data.sample(false,0.1,221).foreach(println)

    println(s"data:${data.getNumPartitions}")

    val data1: RDD[(Int, Int)] = data.mapPartitionsWithIndex(
      (pi, pt) => {
        pt.map(e => (pi, e))
      }
    )


//    val repartition = data1.repartition(8)
    val repartition: RDD[(Int, Int)] = data1.coalesce(3,false)

    val res: RDD[(Int, (Int, Int))] = repartition.mapPartitionsWithIndex(
      (pi, pt) => {
        pt.map(e => (pi, e))
      }

    )
    res

    println(s"data:${res.getNumPartitions}")


    data1.foreach(println)
    println("---------------")
    res.foreach(println)



    while(true){}

  }

}
