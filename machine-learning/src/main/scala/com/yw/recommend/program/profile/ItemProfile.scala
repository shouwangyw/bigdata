package com.yw.recommend.program.profile

import com.yw.recommend.program.util.SparkSessionBase
import org.apache.spark.sql.SaveMode

object ItemProfile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionBase.createSparkSession()
    spark.sql("use program")

    val restDF = spark.sql(
      """
        |select b.id, a.keyword, b.create_date, b.air_date, b.length,
        |b.content_model, b.area, b.language, b.quality, b.is_3d
        |from program.item_keyword a join program.item_info b ON a.item_id = b.id
        |""".stripMargin)
    restDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("item_profile")

    spark.close()
  }
}
