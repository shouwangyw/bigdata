package com.yw.spark.example.structedstreaming.cases

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 读取Socket 数据，将数据写出到mysql中
  */
object Case09_ForeachBatchSink {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node5")
      .option("port", 9999)
      .load()

    val personDF: DataFrame = df.as[String].map(line => {
      val arr: Array[String] = line.split(",")
      (arr(0).toInt, arr(1), arr(2).toInt)
    }).toDF("id", "name", "age")

    val query: StreamingQuery = personDF.writeStream
      .foreachBatch((batchDF: DataFrame, batchID: Long) => {
        println("batchID : " + batchID)
        // 将批次数据写入到 MySQL
        batchDF.write.mode(SaveMode.Append)
          .format("jdbc")
          .option("url", "jdbc:mysql://node2:3306/testdb")
          .option("user", "root")
          .option("password", "123456")
          .option("dbtable", "person")
          .save()
      }).start()

    query.awaitTermination()
  }
}