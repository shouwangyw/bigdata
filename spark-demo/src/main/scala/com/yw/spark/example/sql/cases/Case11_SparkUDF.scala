package com.yw.spark.example.sql.cases

import java.util.regex.Pattern

import org.apache.spark.SparkConf
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @author yangwei
  */
object Case11_SparkUDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.format("csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("header", "true")
      .option("multiLine", "true")
      .load("/Volumes/F/MyGitHub/bigdata/spark-demo/src/main/resources/深圳链家二手房成交明细.csv")

    df.createOrReplaceTempView("house_sale")

    // 注册UDF
    spark.udf.register("house_udf", new UDF1[String, String] {
      val pattern: Pattern = Pattern.compile("^[0-9]*$")

      override def call(input: String): String = {
        val matcher = pattern.matcher(input)
        if (matcher.matches()) input
        else "1990"
      }
    }, DataTypes.StringType)

    // 使用UDF
    spark.sql("select house_udf(house_age) from house_sale limit 200").show()
    spark.stop()
  }
}
