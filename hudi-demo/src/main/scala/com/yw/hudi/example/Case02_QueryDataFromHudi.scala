package com.yw.hudi.example

import org.apache.spark.sql.SparkSession

/**
 * 查询hudi表中的数据
 * @author yangwei
 */
object Case02_QueryDataFromHudi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // 读取的数据路径下如果有分区，会自动发现分区数据,需要使用 * 代替，指定到 parquet 格式数据上层目 录即可。
    spark.read.format("hudi").load("/hudi_data/person_infos/*/*")
      .createTempView("person_infos_view")

    // 查询结果
    val result = spark.sql(
      """
        | select * from person_infos_view
        |""".stripMargin)

    result.show(false)
  }
}
