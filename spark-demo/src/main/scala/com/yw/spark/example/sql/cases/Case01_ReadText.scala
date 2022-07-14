package com.yw.spark.example.sql.cases

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @author yangwei
  */
object Case01_ReadText {
  def main(args: Array[String]): Unit = {
    // 创建 SparkSession
    val ss = SparkSession.builder().appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()

    val df: DataFrame = ss.read.text(this.getClass.getClassLoader.getResource("person.txt").getPath)

    /**
      * 打印schema信息
      * root
      * |-- value: string (nullable = true)
      */
    df.printSchema

    println("----------------")
    println(df.count()) // 4

    /**
      * +------------+
      * |       value|
      * +------------+
      * | 1 youyou 38|
      * |   2 Tony 25|
      * |3 laowang 18|
      * |   4 dali 30|
      * +------------+
      */
    println("----------------")
    df.show()

    ss.stop()
  }
}
