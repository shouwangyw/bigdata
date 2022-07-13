package com.yw.spark.example.cases

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
case class MySearcher(val query: String) {
  def getMatchRddByQuery(rdd: RDD[String]): RDD[String] = {
    rdd.filter(x => x.contains(query))
  }
}

object Case11_Kyro {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
      // 替换默认序列化机制
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册需要使用kryo序列化自定义类
      .registerKryoClasses(Array(classOf[MySearcher]))
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array("hadoop yarn", "hadoop hdfs", "c"))
    val rdd2: RDD[String] = MySearcher("hadoop").getMatchRddByQuery(rdd1)
    rdd2.foreach(println)
  }
}
