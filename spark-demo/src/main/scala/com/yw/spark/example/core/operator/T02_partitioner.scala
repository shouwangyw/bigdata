package com.yw.spark.example.core.operator

import com.yw.spark.example.core.partition.MyPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object T02_partitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array("hello", "hadoop", "hello", "spark"), 1)
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))
    println("rdd2分区数：" + rdd2.partitions.length) // 1
    println("rdd2分区器：" + rdd2.partitioner) // None
    println("------------------------------------")

    val rdd3: RDD[(String, Int)] = rdd2.partitionBy(new MyPartitioner(2))
    println("rdd3分区数：" + rdd3.partitions.length) // 2
    println("rdd3分区器：" + rdd3.partitioner) // Some(com.yw.spark.example.core.partition.MyPartitioner@1603dc2f)
    println("------------------------------------")

    println("rdd3: ")
    val result = rdd3.mapPartitionsWithIndex((idx, it) => {
      it.map(x => (idx, (x._1, x._2)))
    })
    result.foreach(print) // (0,(hello,1))(1,(spark,1))(0,(hadoop,1))(0,(hello,1))
    println("------------------------------------")

    sc.stop()
  }
}
