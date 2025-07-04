package com.yw.hudi.example

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 *
 * @author yangwei
 */
object InsertDataToHudi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    val insertDf = spark.read.json("/hudi_data/insert_data.json")
    // {"id":1,"name":"zs1","age":18,"loc":"beijing","data_dt":"20210709"}
    val partitionDf = insertDf.withColumn("partition_key", concat_ws("-", col("loc"), col("data_dt")))
    partitionDf.show()

    // 将结果保存到hudi中
    insertDf.write.format("org.apache.hudi") // 或者直接写hudi
      // 设置主键列名称
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
      // Hudi默认使用 ts 作为事件时间字段，需要显示指定
      // 当数据主键相同时，对比的字段，保存该字段大的数据
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "data_dt")
      // 指定分区列
//      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "loc")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "partition_key")
      // 并行度设置，默认1500
      .option("hoodie.insert.shuffle.parallelism", 2)
      .option("hoodie.upsert.shuffle.parallelism", 2)
      // 设置表名称
      .option(HoodieWriteConfig.TBL_NAME.key(), "person_infos")
      .mode(SaveMode.Overwrite)
      .save("/hudi_data/person_infos")
  }
}
