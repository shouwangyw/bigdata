package com.yw.spark.example.structedstreaming.cases

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 监控csv 学生信息，统计分数大于60分 班级平均分
  */
object Case11_DataFrameAPI {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    val schema = new StructType().add("id", "integer")
      .add("name", "string")
      .add("cls", "string")
      .add("score", "integer")
    val df: DataFrame = spark.readStream
      .option("sep", ",")
      .schema(schema)
      .csv(".data/studentInfo*.csv")

    df.createTempView("temp")

    val result: DataFrame = spark.sql(
      """
        |select cls,avg(score) as avg_score from temp where score > 60 group by cls
      """.stripMargin)

    //过滤分数大于60分的学生并按照班级分组统计每个班级平均分
    //    val result: DataFrame = df.where("score > 60")
    //      .groupBy("cls")
    //      .avg("score")
    //      .withColumnRenamed("avg(score)", "avg_score")

    val query: StreamingQuery = result.writeStream
      .format("console")
      .outputMode("complete")
      .start()
    query.awaitTermination()

  }
}