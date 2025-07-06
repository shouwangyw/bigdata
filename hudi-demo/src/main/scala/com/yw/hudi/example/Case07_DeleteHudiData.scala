package com.yw.hudi.example

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 删除指定id的hudi数据
 *
 * @author yangwei
 */
object Case07_DeleteHudiData {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // 读取需要删除的数据，只需要准备对应的主键及分区即可，字段保持与Hudi中需要删除的字段名称一致即可
    // 读取的文件中准备了一个主键在Hudi中存在但是分区不在Hudi中的存在的数据，此主键数据在Hudi中不能被删除，需要分区和主键都匹配才能删除
    val deleteDf = spark.read.json("/hudi_data/delete_data.json")

    // 将删除的数据插入到Hudi中
    deleteDf.write.format("hudi")
      // 指定操作模式为delete
      .option(DataSourceWriteOptions.OPERATION.key(), "delete")
      // 指定主键
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "data_dt")
      // 指定分区字段
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "data_dt,loc")
      // 指定表名，这里的表名需要与之前指定的表名保持一致
      .option(HoodieWriteConfig.TBL_NAME.key(), "person_infos")
      .option("hoodie.delete.shuffle.parallelism", "2")
      .option("hoodie.metadata.enable", "false")
      .mode(SaveMode.Append)
      .save("/hudi_data/person_infos")

    // 执行删除后，查询结果
    spark.read.format("hudi").load("/hudi_data/person_infos/*/*")
      .orderBy(col("_hoodie_commit_time")).show(100, false)
  }
}
