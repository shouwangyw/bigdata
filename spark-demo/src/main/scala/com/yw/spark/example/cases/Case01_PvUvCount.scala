package com.yw.spark.example.cases

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object Case01_PvUvCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf 对象
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    // 2. 创建 SparkContext对象
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 3. 读取数据文件
    val dataRDD: RDD[String] = sc.textFile(this.getClass.getClassLoader.getResource("access.log").getPath)

    // 4.1 统计 PV
    val pv: Long = dataRDD.count()
    println(s"PV: $pv")

    val ipsRDD: RDD[String] = dataRDD.map(x => x.split(" ")(0)) // 获取所有的ip地址
    val distinctRDD: RDD[String] = ipsRDD.distinct // 对ip地址进行去重
    // 4.2 统计 UV
    val uv: Long = distinctRDD.count()
    println(s"UV: $uv")

    // 5. 关闭sc
    sc.stop()
  }
}
