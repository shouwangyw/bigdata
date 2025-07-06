package com.yw.hudi.example

import org.apache.hudi.DataSourceReadOptions
import org.apache.spark.sql.SparkSession

/**
 * 增量查询hudi数据
 * @author yangwei
 */
object Case04_IncrementQueryDataFromHudi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("Error")
    import spark.implicits._

    val basePath = "/hudi_data/person_infos"
    // 查询全量数据，查询对应的提交时间，找出倒数第二个时间
    spark.read.format("hudi").load(s"${basePath}/*/*")
      .createTempView("person_infos_view")

    val dt = spark.sql("select distinct(_hoodie_commit_time) as commit_time from person_infos_view order by 1 desc")
      // 这里获取由大到小排序的第二个值
      .map(row => {row.getString(0)}).collect()(1)

    // 增量查询
    /**
     * 指定数据查询的方式，有以下三种：
     * val QUERY_TYPE_SNAPSHOT_OPT_VAL = "snapshot" -- 获取最新所有数据，默认
     * val QUERY_TYPE_READ_OPTIMIZED_OPT_VAL = "read_optimized" -- 获取指定时间戳后的变化数据
     * val QUERY_TYPE_INCREMENTAL_OPT_VAL = "incremental" -- 只查询Base文件中的数据
     *
     * 1) Snapshot mode (obtain latest view, based on row & columnar data)
     * 2) incremental mode (new data since an instantTime)
     * 3) Read Optimized mode (obtain latest view, based on columnar data)
     *
     * Default: snapshot
     */
    val result = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      // 必须指定一个开始查询时间，否则报错
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME.key(), dt)
      .load(s"${basePath}/*/*")

    result.show(false)
  }
}
