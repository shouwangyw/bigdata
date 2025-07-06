package com.yw.hudi.example

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 向HDFS /hudi_data/person_infos中插入数据，两次插入时间间隔至少1分钟
 *
 * @author yangwei
 */
object Case05_AppendDataToHudi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

//    val appendDf = spark.read.json("/hudi_data/append_data1.json")
    val appendDf = spark.read.json("/hudi_data/append_data2.json")

    // 向Hudi中插入数据
    appendDf.write.format("hudi")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "data_dt")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "data_dt,loc")
      .option("hoodie.insert.shuffle.parallelism", 2)
      .option("hoodie.upsert.shuffle.parallelism", 2)
      .option("hoodie.metadata.enable", "false")
      .option(HoodieWriteConfig.TBL_NAME.key(), "person_infos")
      .mode(SaveMode.Append)
      .save("/hudi_data/person_infos")

    spark.read.format("hudi").load("/hudi_data/person_infos/*/*")
      .orderBy(col("_hoodie_commit_time"))
      .show(100, false)
  }
}
