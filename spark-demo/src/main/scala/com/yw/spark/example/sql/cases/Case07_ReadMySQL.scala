package com.yw.spark.example.sql.cases

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 使用 SparkSQL读写MySQL表中的数据
  *
  * @author yangwei
  */
object Case07_ReadMySQL {
  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkConf 对象
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")

    // 2. 创建 SparkSession 对象
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 3. 创建 DataFrame
    val url = "jdbc:mysql://192.168.254.132:3306/mydb?characterEncoding=UTF-8"
    val tableName = "jobdetail"
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")
    val mysqlDF: DataFrame = spark.read.jdbc(url, tableName, props)

    // 4. 读取 MySQL 表中的数据
    // 4.1 打印schema信息
    mysqlDF.printSchema()
    // 4.2 展示数据
    mysqlDF.show()
    // 4.3 将dataFrame注册成表
    mysqlDF.createTempView("job_detail")

    spark.sql("select * from job_detail where city = '广东'").show()

    spark.stop()
  }
}
