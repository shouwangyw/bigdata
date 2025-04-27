package com.yw.spark.example.structedstreaming.cases

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Structured Streaming监控目录 csv格式数据
  *
  * @author yangwei
  */
object Case05_ReadJsonSource {
  def main(args: Array[String]): Unit = {
    // 1. 创建对象
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    // 2. 创建 json 数据schema
    val userSchema: StructType = new StructType().add("id", "integer")
      .add("name", "string")
      .add("age", "integer")
      .add("gender", "string")


    val result: DataFrame = spark.readStream
      .schema(userSchema)
      .json(".data/")

    val query: StreamingQuery = result.writeStream
      .format("console")
      .start()

    query.awaitTermination()

  }
}
