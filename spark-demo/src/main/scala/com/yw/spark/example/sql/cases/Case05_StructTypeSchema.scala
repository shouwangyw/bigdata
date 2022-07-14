package com.yw.spark.example.sql.cases

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  *
  * @author yangwei
  */
object Case05_StructTypeSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val rdd: RDD[Array[String]] = sc.textFile(this.getClass.getClassLoader.getResource("person.txt").getPath)
      .map(x => x.split(" "))

    // 将rdd与Row对象关联
    val rowRDD: RDD[Row] = rdd.map(x => Row(x(0), x(1), x(2).toInt))

    // 指定dataFrame的schema信息，这里指定的字段个数和类型必须要跟Row对象保持一致
    val schema = StructType(
      StructField("id", StringType) ::
        StructField("name", StringType) ::
        StructField("age", IntegerType) :: Nil
    )

    // 利用rdd生成DataFrame
    val df: DataFrame = spark.createDataFrame(rowRDD, schema)

    /**
      * root
      * |-- id: string (nullable = true)
      * |-- name: string (nullable = true)
      * |-- age: integer (nullable = true)
      */
    df.printSchema

    /**
      * +---+-------+---+
      * | id|   name|age|
      * +---+-------+---+
      * |  1| youyou| 38|
      * |  2|   Tony| 25|
      * |  3|laowang| 18|
      * |  4|   dali| 30|
      * +---+-------+---+
      */
    df.show()

    // 用sql的方式查询结构化数据
    df.createTempView("person")

    /**
      * +---+-------+---+
      * | id|   name|age|
      * +---+-------+---+
      * |  1| youyou| 38|
      * |  2|   Tony| 25|
      * |  3|laowang| 18|
      * |  4|   dali| 30|
      * +---+-------+---+
      */
    spark.sql("select * from person").show()

    spark.stop()
  }
}
