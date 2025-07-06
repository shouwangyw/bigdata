package com.yw.hudi.example

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * COW 默认情况下，每次更新数据 Commit 都会基于之前 parquet 文件生成一个新的 Parquet Base 文件数据，
 * 默认历史 parquet 文件数为 10，当超过 10 个后会自动删除 旧的版本，
 * 可以通过参数“hoodie.cleaner.commits.retained”来控制保留的 FileID 版本文件数，默认是 10。
 *
 * @author yangwei
 */
object Case11_COW {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")

    // 依次执行10次数据插入
//    val insertDf = spark.read.json("/hudi_data/insert/data1.json")
//    val insertDf = spark.read.json("/hudi_data/insert/data2.json")
//    val insertDf = spark.read.json("/hudi_data/insert/data3.json")
//    val insertDf = spark.read.json("/hudi_data/insert/data4.json")
//    val insertDf = spark.read.json("/hudi_data/insert/data5.json")
//    val insertDf = spark.read.json("/hudi_data/insert/data6.json")
//    val insertDf = spark.read.json("/hudi_data/insert/data7.json")
//    val insertDf = spark.read.json("/hudi_data/insert/data8.json")
//    val insertDf = spark.read.json("/hudi_data/insert/data9.json")
//    val insertDf = spark.read.json("/hudi_data/insert/data10.json")
//
//    insertDf.write.format("hudi")
//      // 设置COW模式
//      .option(DataSourceWriteOptions.TABLE_TYPE.key(), DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
//      // 根据commit提交次数计算保留多少个fileID版本文件，默认10
//      .option("hoodie.cleaner.commits.retained", "3")
//      // 设置主键列名称
//      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
//      // 当数据主键相同时，对比的字段，保存该字段大的数据
//      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "data_dt")
//      .option("hoodie.insert.shuffle.parallelism", "2")
//      .option("hoodie.upsert.shuffle.parallelism", "2")
//      .option("hoodie.metadata.enable", "false")
//      // 设置表名称
//      .option(HoodieWriteConfig.TBL_NAME.key(), "test_person")
//      .mode(SaveMode.Append)
//      .save("/hudi_data/test_person")

    // 查询结果数据
    spark.read.format("hudi")
      // 全量读取
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load("/hudi_data/test_person")
      .show(100, false)
  }
}
