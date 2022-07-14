package com.yw.spark.example.sql.cases

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @author yangwei
  */
object Case04_ReadParquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df: DataFrame = spark.read.parquet(this.getClass.getClassLoader.getResource("users.parquet").getPath)

    /**
      * root
      * |-- name: string (nullable = true)
      * |-- favorite_color: string (nullable = true)
      * |-- favorite_numbers: array (nullable = true)
      * |    |-- element: integer (containsNull = true)
      */
    df.printSchema

    /**
      * +------+--------------+----------------+
      * |  name|favorite_color|favorite_numbers|
      * +------+--------------+----------------+
      * |Alyssa|          null|  [3, 9, 15, 20]|
      * |   Ben|           red|              []|
      * +------+--------------+----------------+
      */
    df.show

    spark.stop()
  }
}
