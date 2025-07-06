package com.yw.hudi.example

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 更新hudi表中的数据
 * @author yangwei
 */
object Case03_UpdateDataToHudi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")
//    println(org.apache.hadoop.hdfs.client.HdfsDataInputStream.getProtectionDomain.getCodeSource.getLocation)
    // 读取数据
    val insertDf = spark.read.json("/hudi_data/update_data.json")

    // 将结果保存到hudi中
    insertDf.write.format("hudi")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "data_dt")
      // 指定分区列
//      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "loc")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "data_dt,loc")
      // 并行度设置，默认1500
      .option("hoodie.insert.shuffle.parallelism", 2)
      .option("hoodie.upsert.shuffle.parallelism", 2)
      // 临时解决包依赖冲突：java.lang.NoSuchMethodError: org.apache.hadoop.hdfs.client.HdfsDataInputStream.getReadStatistics
      .option("hoodie.metadata.enable", "false")
      // 设置表名称
      .option(HoodieWriteConfig.TBL_NAME.key(), "person_infos")
      .mode(SaveMode.Append)
      .save("/hudi_data/person_infos")
  }
}
