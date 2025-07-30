package com.yw.recommend.program.profile

import com.yw.recommend.program.util.SparkSessionBase
import org.apache.spark.sql.SaveMode

object ItemProfile {
  def main(args: Array[String]): Unit = {
    val session = SparkSessionBase.createSparkSession()
    session.sql("use tmp_program")
    val sqlText = "SELECT b.id, a.keyword, b.create_date, b.air_date, b.length " +
      ", b.content_model, b.area, b.language, b.quality, b.is_3d " +
      "FROM tmp_program.item_keyword a " +
      "JOIN program.item_info b ON a.item_id = b.id ";
    val restDF = session.sql(sqlText)
    restDF
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("item_profile")

    session.close()
  }
}
