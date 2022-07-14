package com.yw.spark.example.sql.cases

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @author yangwei
  */
object Case03_ReadJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df: DataFrame = spark.read.json(this.getClass.getClassLoader.getResource("person.json").getPath)

    /**
      * root
      * |-- age: long (nullable = true)
      * |-- name: string (nullable = true)
      */
    df.printSchema

    println("--------------")

    /**
      * +----+-------+
      * | age|   name|
      * +----+-------+
      * |null|Michael|
      * |  30|   Andy|
      * |  19| Justin|
      * +----+-------+
      */
    df.show()

    spark.stop()
  }
}
