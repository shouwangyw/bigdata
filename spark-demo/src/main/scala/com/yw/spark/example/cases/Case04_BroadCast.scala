package com.yw.spark.example.cases

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object Case04_BroadCast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 1. 读取商品数据: 将商品文件数据内容作为广播变量
    val pdtRDD: RDD[String] = sc.textFile(this.getClass.getClassLoader.getResource("pdts.txt").getPath)
    // 将数据收集起来
    val mapPdt: collection.Map[String, String] = pdtRDD.map(x => {
      (x.split(",")(0), x)
    }).collectAsMap()
    // 开始广播
    val broadcastValue: Broadcast[collection.Map[String, String]] = sc.broadcast(mapPdt)

    // 2. 读取订单数据
    val ordersRDD: RDD[String] = sc.textFile(this.getClass.getClassLoader.getResource("orders.txt").getPath)
    // 订单数据rdd进行拼接商品数据
    val pdtAndOrderRDD: RDD[String] = ordersRDD.mapPartitions(eachPartition => {
      val getBroadcastMap: collection.Map[String, String] = broadcastValue.value
      val finalStr: Iterator[String] = eachPartition.map(eachLine => {
        val ordersGet: Array[String] = eachLine.split(",")
        val getPdt: String = getBroadcastMap.getOrElse(ordersGet(2), "")
        eachLine + "\t" + getPdt
      })
      finalStr
    })
    for (item <- pdtAndOrderRDD.collect()) {
      println(item)
    }
    sc.stop()
  }
}
