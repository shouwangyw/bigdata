package com.yw.spark.example.sql.cases

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

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

    // 3. 读取 MySQL 表
    val url = "jdbc:mysql://192.168.254.132:3306/mydb?characterEncoding=UTF-8"
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")
    val studentDF: DataFrame = spark.read.jdbc(url, "students", props)
    val scoreDF: DataFrame = spark.read.jdbc(url, "scores", props)

    // 4. 将DataFrame注册成临时视图
    studentDF.createTempView("students")
    scoreDF.createTempView("scores")

    // 5. 使用Spark SQL查询数据
    val resultDF: DataFrame = spark.sql(
      """
        |SELECT tmp.class, SUM(tmp.degree), AVG(tmp.degree)
        |FROM (
        |	SELECT
        |		students.sno AS ssno,
        |		students.sname,
        |		students.ssex,
        |		students.sbirthday,
        |		students.class,
        |		scores.sno,
        |		scores.degree,
        |		scores.cno
        |	FROM students
        |	LEFT JOIN scores ON students.sno = scores.sno
        |	WHERE degree > 60 AND sbirthday > '1973-01-01 00:00:00'
        |) tmp GROUP BY tmp.class
      """.stripMargin)

    // 6. 查看执行计划
    resultDF.explain(true)

    // 7. 展示数据
    resultDF.show()

    spark.stop()
  }
}
