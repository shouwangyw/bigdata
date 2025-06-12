package com.msb.bigdata.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 算子综合应用：分组取 topN（二次排序）
 *
 * 求相同月份中，温度最高的2天
 * 数据状况如下：
 * 2019-6-1	39
 * 2019-5-21	33
 * 2019-6-1	38
 * 2019-6-2	31
 * 2018-3-11	18
 * 2018-4-23	22
 * 1970-8-23	23
 * 1970-8-8	32
 */
object L06_RDD_over {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName))
    sc.setLogLevel("ERROR")

    // 0. 读取数据文件，转成一个tuple4数据集，后面针对该数据集进行处理
    val file: RDD[String] = sc.textFile("spark-demo/data/tqdata")
    // line: 2019-6-1	39
    val data = file.map(line => line.split("\t")).map(arr => {
      val arrs: Array[String] = arr(0).split("-")
      //(year,month,day,wd)
      (arrs(0).toInt, arrs(1).toInt, arrs(2).toInt, arr(1).toInt)
    })

    implicit val defaultOrder = new Ordering[(Int, Int)] {
      override def compare(x: (Int, Int), y: (Int, Int)) = y._2.compareTo(x._2)
    }

    println("-->> 1. 方法一：直接使用 groupByKey 实现对数据的去重、排序 <<--")
    data
      // 1.1 先进行一次转换 (year,month,day,wd) → ((year,month),(day,wd)) (K, V)格式，针对`K:年月`进行分组
      .map(t4 => ((t4._1, t4._2), (t4._3, t4._4))).groupByKey()
      // 1.2 针对`V:日温度`进行处理
      .mapValues(value => {
        val map = new mutable.HashMap[Int, Int]()
        // 利用HashMap去重相同天的温度，并且HashMap的值最终留下的当天的最高温度
        value.foreach(x => if (map.getOrElse(x._1, 0) < x._2) map.put(x._1, x._2))
        // HashMap转List后对温度值进行排序（从大到小 - 倒序）
        map.toList.sorted((x: (Int, Int), y: (Int, Int)) => y._2.compareTo(x._2))
      }).foreach(println)
    // 方法一会产生1次shuffle，同时会出现的问题：
    // 1. groupByKey在数据量很大的情况下，会发生OOM
    // 2. 针对每日温度处理时，额外使用了HashMap，内存使用上也可能会出现OOM

    println("-->> 2. 方法二：使用 reduceByKey 取max间接达到去重 <<--")
    data
      // 2.1 先进行一次转换 (year,month,day,wd) → ((year,month,day),wd) (K, V)格式，针对`K:年月日`进行分组
      .map(t4 => ((t4._1, t4._2, t4._3), t4._4))
      // 2.2 针对`V:温度`取最大值，实现去重
      .reduceByKey((oldVal: Int, newVal: Int) => if (newVal > oldVal) newVal else oldVal)
      // 2.3 格式转换 ((year,month,day),wd) → ((year,month),(day,wd))
      .map(t2 => ((t2._1._1, t2._1._2), (t2._1._3, t2._2)))
      // 2.4 再进行分组，对`V:温度`进行排序取topN
      .groupByKey()
      .mapValues(value => value.toList.sorted.take(2))
      .foreach(println)
    // 方法二会产生2次shuffle，虽然通过reduceByKey去重大大减少了数据量（同一个月最多31条数据）
    // 但最终还是用了groupByKey，如果是其它场景分组后数据量大，仍然有可能出现OOM

    println("-->> 3. 方法三：使用 reduceByKey 取max间接达到去重，使用 sortByKey 排序 <<--")
    data
      // 3.1 对(year,month,wd)进行排序，相同年月的温度会从大到小排序
      .sortBy(t4 => (t4._1, t4._2, t4._4), false)
      // 3.2 对排序后的数据转换格式 ((year,month,day),wd) (K, V)格式
      .map(t4 => ((t4._1, t4._2, t4._3), t4._4))
      // 3.3 针对`V:温度`取最大值，实现去重
//      .reduceByKey((oldVal: Int, newVal: Int) => if (newVal > oldVal) newVal else oldVal)
      .reduceByKey((oldVal: Int, newVal: Int) => oldVal)
      // 3.4 格式转换 ((year,month,day),wd) → ((year,month),(day,wd))
      .map(t2 => ((t2._1._1, t2._1._2), (t2._1._3, t2._2)))
      // 3.5 分组
      .groupByKey().foreach(println)
    // 方法三会产生3次shuffle，先通过sortByKey进行排序，然后reduceByKey去重取相同年月日
    // 最终的结果是数据乱掉了
    // 注意：多级shuffle中，关注后续shuffle的key一定是前置rdd key的子集

    println("-->> 4. 方法四：使用 sortByKey 排序，不破坏多级shuffle的key的子集关系 <<--")
    data
      // 4.1 对(year,month,wd)进行排序，相同年月的温度会从大到小排序
      .sortBy(t4 => (t4._1, t4._2, t4._4), false)
      // 4.2 格式转换 (year,month,wd) → ((year,month),(day,wd))
      .map(t4 => ((t4._1, t4._2), (t4._3, t4._4)))
      // 4.3 分组后数据不会乱
      .groupByKey()
      .foreach(println)

    /**
     * 分布式计算的核心思想：调优天下无敌 combineByKey
     * 分布式是并行的，离线批量计算有个特征就是后续步骤(stage)依赖前一步骤(stage)
     * 如果前一步骤(stage)能够加上正确的combineByKey
     * 我们自定的combineByKey的函数，是尽量压缩内存中的数据
     */
    println("-->> 5. 方法五：使用 combineByKey 优化 <<--")
    data
      // 5.1 先进行一次转换 (year,month,day,wd) → ((year,month),(day,wd)) (K, V)格式
      .map(t4 => ((t4._1, t4._2), (t4._3, t4._4)))
      // 5.2 使用 combineByKey 优化
      .combineByKey(
        // 第一条数据怎么存放？v1: (day,wd)
        // 初始化一个数组，包含三个元素
        (v1: (Int, Int)) => Array(v1, (0, 0)),
        // 第二条以及后续数据怎么存放？
        (oldVs: Array[(Int, Int)], newV: (Int, Int)) => {
          // 实现去重+排序
          // 考虑新进来的元素特征：① `日`相同: 温度大、温度小，② `日`不同
          var flag = 0 // 有0, 1, 2三种状态
          for (i <- oldVs.indices) {
            if (oldVs(i)._1 == newV._1) {
              if (oldVs(i)._2 < newV._2) {
                flag = 1
                oldVs(i) = newV
              } else {
                flag  = 2
              }
            }
          }
          if (flag == 0) {
            oldVs(oldVs.length - 1) = newV
          }
//          oldVs.sorted
          scala.util.Sorting.quickSort(oldVs)
          oldVs
        },
        // 数据怎么溢写合并
        (v1: Array[(Int, Int)], v2: Array[(Int, Int)]) => {
          // 关注去重
          val union: Array[(Int, Int)] = v1.union(v2)
          union.sorted
        }
      ).map(x => (x._1, x._2.toList)).foreach(println)

    sc.parallelize(List(
      "hello world",
      "hello spark",
      "hello world",
      "hello hadoop",
      "hello world",
      "hello msb",
      "hello world"
    )).flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
//      .map(x=>(x._1, x._2 * 10))  // 会多一次shuffle

      .mapValues(x => x * 10)
      .groupByKey()
      .foreach(println)

    while(true){

    }
  }
}
