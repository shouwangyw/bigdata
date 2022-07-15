package com.yw.spark.example.sql.cases

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *
  * @author yangwei
  */
// 定义一个样例类
case class Person(id: String, name: String, age: Int)

object Case06_SparkConversion {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    // 隐式转换
    import spark.implicits._

    val rdd = sc.textFile(this.getClass.getClassLoader.getResource("person.txt").getPath)
      .map(x => x.split(" "))

    // 把rdd与样例类进行关联
    val personRDD = rdd.map(x => Person(x(0), x(1), x(2).toInt))

    // 1. rdd -> df
    val df1 = personRDD.toDF
    df1.show

    // 2. rdd -> ds
    val ds1 = personRDD.toDS
    ds1.show

    // 3. df -> rdd
    val rdd1 = df1.rdd
    println(rdd1.collect.toList)

    // 4. ds -> rdd
    val rdd2 = ds1.rdd
    println(rdd2.collect.toList)

    // 5. ds -> df
    val df2: DataFrame = ds1.toDF
    df2.show

    // df -> ds
    val ds2: Dataset[Person] = df2.as[Person]
    ds2.show

    spark.stop()
  }
}
