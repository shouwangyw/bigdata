package com.yw.spark.example.structedstreaming.cases

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 监控csv 学生信息，统计分数大于60分 班级平均分
  */
object Case12_DataSetAPI {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("Error")

    val schema = new StructType().add("id", "integer")
      .add("name", "string")
      .add("cls", "string")
      .add("score", "integer")

    val df: DataFrame = spark.readStream
      .option("sep", ",")
      .schema(schema)
      .csv(".data/studentInfo*.csv")

    import org.apache.spark.sql.expressions.scalalang.typed
    val result: Dataset[String] = df.as[Students]
      .filter(student => {
        student.score > 60
      })
      .groupByKey(_.cls)
      .agg(typed.avg(_.score))
      .map(tp => {
        tp._1 + "-" + tp._2
      })
    result.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }
}

case class Students(id: Int, name: String, cls: String, score: Int)
