package com.yw.spark.example.structedstreaming.cases

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 读取scoket 数据写入memory内存，再读取
  */
object Case08_MemorySink {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    val result: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node5")
      .option("port", 9999)
      .load()

    val query: StreamingQuery = result.writeStream
      .format("memory")
      .queryName("mytable")
      .start()

    // 查询内存中表数据
    while (true) {
      Thread.sleep(2000)
      spark.sql(
        """
          |select * from mytable
        """.stripMargin).show()

      query.awaitTermination()
    }
  }
}