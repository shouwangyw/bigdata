package com.yw.hudi.example

import org.apache.hudi.DataSourceReadOptions
import org.apache.spark.sql.SparkSession

/**
 * 按指定时间段查询Hudi中的数据
 *
 * @author yangwei
 */
object Case06_QueryDataByRangeTimeFromHudi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // 指定时间段，查询hudi中的数据
//    val beginTime = "000"
    val beginTime = "20250705171928349"
    val endTime = "20250705193908898"

    val result = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key(), beginTime)
      .option(DataSourceReadOptions.END_INSTANTTIME.key(), endTime)
      .load("/hudi_data/person_infos/*/*")

    result.createTempView("person_infos_tmp")
    spark.sql(
      """
        | select * from person_infos_tmp order by _hoodie_commit_time
        |""".stripMargin)
      .show(100, false)
  }
}
