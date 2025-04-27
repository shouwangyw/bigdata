package com.yw.spark.example.structedstreaming.cases

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Structured Streaming 流与静态数据join 关联
  */
object Case17_StreamAndStaticJoin {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName)
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._

    // 创建 批量的DataFrame
    val list = List[String](
      "{\"aid\":1,\"name\":\"zs\",\"age\":18}",
      "{\"aid\":2,\"name\":\"ls\",\"age\":19}",
      "{\"aid\":3,\"name\":\"ww\",\"age\":20}",
      "{\"aid\":4,\"name\":\"ml\",\"age\":21}"
    )
    val personInfo: DataFrame = spark.read.json(list.toDS())

    // 创建流式数据
    /**
      * 1,zs,100
      * 2,ls,200
      * 3,ww,300
      * 5,tq,500
      */
    val scoreInfo: DataFrame = spark.readStream
      .format("socket")
      .option("host", "node5")
      .option("port", 9999)
      .load()
      .as[String]
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0).toInt, arr(1), arr(2).toInt)
      }).toDF("bid", "name", "score")

    // 将流数据与静态数据进行关联
    val result: DataFrame = scoreInfo.join(personInfo,
      scoreInfo.col("bid") === personInfo.col("aid"),
      "left_semi")

    result.printSchema()

    result.writeStream
      .format("console")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()
      .awaitTermination()
  }
}
