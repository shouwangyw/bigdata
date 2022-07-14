package com.yw.spark.example.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object T01_partitionBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("a", 1), ("a", 2), ("b", 1), ("b", 3), ("c", 1), ("e", 1)), 2)
    println("rdd1分区数：" + rdd1.partitions.length)  // 2
    println("rdd1分区器：" + rdd1.partitioner)        // None
    println("------------------------------------")

    val rdd2: RDD[(String, Int)] = rdd1.repartition(4)
    println("rdd2分区数：" + rdd2.partitions.length)  // 4
    println("rdd2分区器：" + rdd2.partitioner)        // None
    println("------------------------------------")

    val rdd3: RDD[(String, Int)] = rdd1.partitionBy(new HashPartitioner(4))
    println("rdd3分区数：" + rdd3.partitions.length)  // 4
    println("rdd3分区器：" + rdd3.partitioner)        // Some(org.apache.spark.HashPartitioner@4)
    println("------------------------------------")

    println("rdd2: ")
    rdd2.glom().mapPartitionsWithIndex((idx, it) => {
      it.map(x => (idx, x.mkString(",")))
    }).foreach(print) // (3,(a,1),(e,1))(2,(c,1))(0,(a,2))(1,(b,1),(b,3))
    println("------------------------------------")

    println("rdd3: ")
    rdd3.glom().mapPartitionsWithIndex((idx, it) => {
      it.map(x => (idx, x.mkString(",")))
    }).foreach(print) //  key相同的肯定在一个分区: (2,(b,1),(b,3)) (1,(a,1),(a,2),(e,1)) (0,)(3,(c,1))
    println("------------------------------------")

    sc.stop()
  }
}
