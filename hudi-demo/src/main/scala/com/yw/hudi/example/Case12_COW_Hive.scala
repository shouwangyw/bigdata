package com.yw.hudi.example

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.{HiveSyncConfigHolder, MultiPartKeysValueExtractor}
import org.apache.hudi.sync.common.HoodieSyncConfig
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * COW模式-SparkSQL代码写入Hudi同时映射Hive表
 * @author yangwei
 */
object Case12_COW_Hive {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

//    val insertDf = spark.read.json("/hudi_data/insert_data.json")
    val insertDf = spark.read.json("/hudi_data/update_data.json")

    insertDf.write.format("hudi")
      // 设置写入模式，默认为COW
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "data_dt")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "loc")
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .option("hoodie.metadata.enable", "false")
      .option(HoodieWriteConfig.TBL_NAME.key(), "person_infos")
      // 关于Hive的设置
      // 指定HiveServer2连接URL
      .option(HiveSyncConfigHolder.HIVE_URL.key(), "jdbc:hive2://node1:10000")
      .option(HiveSyncConfigHolder.HIVE_USER.key(), "root")
      .option(HiveSyncConfigHolder.HIVE_PASS.key(), "root")
      // 指定Hive对应的库名
      .option(HoodieSyncConfig.META_SYNC_DATABASE_NAME.key(), "default")
      // 指定Hive映射的表名称
      .option(HoodieSyncConfig.META_SYNC_TABLE_NAME.key(), "info1")
      // 指定Hive映射表对应的分区字段
      .option(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS.key(), "loc")
      // 当设置为true时，注册/同步表到Hive Metastore；默认是false，这里就是自动创建表
      .option(HoodieSyncConfig.META_SYNC_ENABLED.key(), "true")
      // 如果分区格式不是yyyy/mm.dd，需要指定解析类将分区列解析到Hive中
      .option(HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS.key(), classOf[MultiPartKeysValueExtractor].getName)
      .mode(SaveMode.Append)
      .save("/hudi_data/person_infos")
  }
}
