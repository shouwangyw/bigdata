package com.yw.spark.example.core.cases

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object Case02_VisitTopN {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf 对象
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")

    // 2. 创建 SparkContext对象
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 3. 读取数据文件
    val dataRDD: RDD[String] = sc.textFile(this.getClass.getClassLoader.getResource("access.log").getPath)

    // 4. 对数据进行过滤
    val filterRDD: RDD[String] = dataRDD.filter(x => x.split(" ").length > 10)

    // 5. 获取每一个条数据中的url地址链接
    val urlsRDD: RDD[String] = filterRDD.map(x => x.split(" ")(10))

    // 6. 过滤掉不是 http 的请求
    val fUrlRDD: RDD[String] = urlsRDD.filter(_.contains("http"))

    // 7. 把每一个URL统计为1
    val urlAndOneRDD: RDD[(String, Int)] = fUrlRDD.map(x => (x, 1))

    // 8. 相同的url出现1进行累加
    val result: RDD[(String, Int)] = urlAndOneRDD.reduceByKey(_ + _)

    // 9. 对url出现的次数进行排序----降序
    val sortRDD: RDD[(String, Int)] = result.sortBy(_._2, false)

    // 10. 取出url出现次数最多的前5位
    val top5: Array[(String, Int)] = sortRDD.take(5)
    top5.foreach(println)

    sc.stop()
  }
}
