package com.yw.spark.example.sql.cases

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @author yangwei
  */
object Case09_SparkSQLOnHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]")
      .enableHiveSupport() // 启用hive
      .config("spark.sql.warehouse.dir", "hdfs://node01:8020/user/hive/warehouse")
      .getOrCreate()

    val df: DataFrame = spark.sql("select * from student")
    df.show()
    // 直接写表达式，通过 insert into 插入
    df.write.saveAsTable("student1")
    spark.sql("insert into student1 select * from student")
  }
}
