package com.yw.spark.example.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object T05_aggrateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val rdd1: RDD[String] = sc.parallelize(Array("hello", "hadoop", "hello", "spark"), 1)
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))

    // 写法一：
    val seqOp = (partSum: Int, value: Int) => {
      partSum + value
    }
    val combOp = (partSumThis: Int, partSumOther: Int) => {
      partSumThis + partSumOther
    }
    rdd2.aggregateByKey(0)(seqOp, combOp)
      .foreach(x => print(x + "\t")) // (spark,1)	(hadoop,1)	(hello,2)
    println
    println("------------------------------------")

    // 写法二：柯里化，好处是可以进行类型推断
    rdd2.aggregateByKey(0)((partSum, value) => {
      partSum + value
    }, (partSumThis, partSumOther) => {
      partSumThis + partSumOther
    }).foreach(x => print(x + "\t")) // (spark,1)	(hadoop,1)	(hello,2)
    println
    println("------------------------------------")

    // 写法三：简化
    val result: RDD[(String, Int)] = rdd2.aggregateByKey(0)(_ + _, _ + _)
    result.foreach(x => print(x + "\t")) // (spark,1)	(hadoop,1)	(hello,2)
    println
    println("------------------------------------")

    sc.stop()
  }
}
