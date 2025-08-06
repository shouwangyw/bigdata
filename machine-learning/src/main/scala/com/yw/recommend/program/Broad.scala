package com.yw.recommend.program

import util.SparkSessionBase

object Broad {
  def main(args: Array[String]): Unit = {
    val session = SparkSessionBase.createSparkSession()
    val sc = session.sparkContext

    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8))

    /**
      * NotSerializableException: org.apache.spark.SparkContext
      *  sc是在Driver进程（JVM进程）中
      *  网络传输
      *  不能创建广播变量
      */
    rdd.map(x=>{
      val broad = sc.broadcast("1")
      x
    }).foreach(println)
    rdd.distinct()
  }
}
