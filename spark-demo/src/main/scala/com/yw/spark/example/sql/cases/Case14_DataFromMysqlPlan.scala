package com.yw.spark.example.sql.cases

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  *
  * @author yangwei
  */
object Case14_DataFromMysqlPlan {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf 对象
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")

    // 2. 创建 SparkSession 对象
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // 3. 读取 MySQL 表中的数据

  }
}
