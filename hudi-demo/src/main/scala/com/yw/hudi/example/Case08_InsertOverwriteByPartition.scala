package com.yw.hudi.example

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 更新Hudi表分区中的数据
 *
 * @author yangwei
 */
object Case08_InsertOverwriteByPartition {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // 读取需要替换的数据，将beijing分区数据替换成2条，将chongqing分区数据替换成1条
    val overwriteDf = spark.read.json("/hudi_data/overwrite.json")

    // 写入hudi表person_infos，替换分区
    overwriteDf.write.format("hudi")
      .option(DataSourceWriteOptions.OPERATION.key(), "insert_overwrite")
      // 设置主键名称
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
      // 当数据主键相同时，对比的字段保存该字段大的数据
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "data_dt")
      // 指定分区列
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "loc")
      // 设置并行度
      .option("hoodie.insert.shuffle.parallelism", 2)
      .option("hoodie.upsert.shuffle.parallelism", 2)
      .option("hoodie.metadata.enable", "false")
      // 表名称设置
      .option(HoodieWriteConfig.TBL_NAME.key(), "person_infos")
      .mode(SaveMode.Append)
      .save("/hudi_data/person_infos")

    // 写入完成之后，查询hudi数据
    val result = spark.read.format("hudi").load("/hudi_data/person_infos/*/*")

    result.show(100, false)
  }
}
