package com.yw.hudi.example

import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * Spark操作 Hudi Merge On Read
 *
 * @author yangwei
 */
object Case10_MOR {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

//    val insertDf = spark.read.json("/hudi_data/insert_data.json")
//
//    insertDf.write.format("hudi")
//      // 设置表模式为MOR
//      .option(DataSourceWriteOptions.TABLE_TYPE.key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
//      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
//      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "data_dt")
//      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "loc")
//      .option("hoodie.insert.shuffle.parallelism", "2")
//      .option("hoodie.upsert.shuffle.parallelism", "2")
//      // 设置表名称
//      .option(HoodieWriteConfig.TBL_NAME.key(), "person_infos")
//      .mode(SaveMode.Append)
//      .save("/hudi_data/person_infos")

//    val updateDf = spark.read.json("/hudi_data/update_data.json")
//
//    updateDf.write.format("hudi")
//      // 设置表模式为MOR
//      .option(DataSourceWriteOptions.TABLE_TYPE.key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
//      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
//      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "data_dt")
//      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "loc")
//      .option("hoodie.insert.shuffle.parallelism", "2")
//      .option("hoodie.upsert.shuffle.parallelism", "2")
//      .option("hoodie.metadata.enable", "false")
//      // 设置表名称
//      .option(HoodieWriteConfig.TBL_NAME.key(), "person_infos")
//      .mode(SaveMode.Append)
//      .save("/hudi_data/person_infos")

    // 使用不同模式查询 MOR 表中的数据
//    // 1. Snapshot 模式查询
//    spark.read.format("hudi")
//      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
//      .load("/hudi_data/person_infos/*/*")
//      .show(100, false)

//    // 2. incremental 模式查询，查询指定时间戳后的数据
//    spark.read.format("hudi")
//      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
//      // 必须指定一个查询开始时间，否则会报错
//      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key(), "20250705214532065")
//      .load("/hudi_data/person_infos/*/*")
//      .show(100, false)

//    // 3. Read Optimized 模式查询，查询 Base 中的数据，不会查询 Log 中的数据
//    spark.read.format("hudi")
//      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
//      .load("/hudi_data/person_infos/*/*")
//      .show(100, false)

//    val insertDf = spark.read.json("/hudi_data/insert_data.json")
    val insertDf = spark.read.json("/hudi_data/update_data.json")

    insertDf.write.format("hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      // 根据commit提交次数计算保留多少个 fileID 版本文件，默认10
      .option("hoodie.cleaner.commits.retained", "3")
      // 默认false：是否在一个事务完成后内联执行压缩操作
      .option("hoodie.compact.inline", "true")
      // 设置提交多少次后触发压缩策略，默认5
      .option("hoodie.compact.inline.max.delta.commits", "2")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "data_dt")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option("hoodie.metadata.enable", "false")
      .option(HoodieWriteConfig.TBL_NAME.key(), "test_person")
      .mode(SaveMode.Append)
      .save("/hudi_data/test_person")

    // 查询结果
    spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load("/hudi_data/test_person")
      .show(100, false)
  }
}
