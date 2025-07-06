package com.yw.hudi.example

import org.apache.spark.sql.SparkSession

/**
 *
 * @author yangwei
 */
object Case14_SparkSql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(this.getClass.getSimpleName)
      .config("hive.metastore.uris", "thrift://node1:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("use default")
    spark.sql(
      """
        | select id, name, age, loc, data_dt from person3_rt
        |""".stripMargin).show()
    spark.sql(
      """
        | select sum(age) as totalAge from person3_rt
        |""".stripMargin).show()
  }
}
