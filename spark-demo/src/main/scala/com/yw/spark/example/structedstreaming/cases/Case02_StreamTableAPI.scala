package com.yw.spark.example.structedstreaming.cases

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 读取socket数据写入流表，再从流表中读取数据展示到控制台
  *
  * @author yangwei
  */
object Case02_StreamTableAPI {
  def main(args: Array[String]): Unit = {
    // 1. 创建对象
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      // Spark利用warehouse路径维护Spark临时表元数据和数据信息
      .config("spark.sql.warehouse.dir",".tmp/spark-warehouse")
//      .config("spark.sql.streaming.checkpointLocation","xxx")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("Error")

    // 2. 读取socket数据，注册流表
    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node5")
      .option("port", 9999)
      .load()

    // 3. 对df进行转换
    val personInfo: DataFrame = df.as[String]
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0).toInt, arr(1), arr(2).toInt)
      }).toDF("id", "name", "age")

    // 4. 将以上 personInfo 写入到流表中
    personInfo.writeStream
      .option("checkpointLocation", s".checkpoint/${System.currentTimeMillis()}")
      .toTable("mytbl")

    import org.apache.spark.sql.functions._

    // 5. 读取 mytbl 流表中的数据
    val query: StreamingQuery = spark.readStream
      .table("mytbl")
      .withColumn("new_age", col("age").plus(10))
      .select("id", "name", "age", "new_age")
      .writeStream
      .format("console")
      .start()

    query.awaitTermination()
  }
}
