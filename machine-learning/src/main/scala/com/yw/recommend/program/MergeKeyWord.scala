package com.yw.recommend.program

import com.yw.recommend.program.util.SparkSessionBase
import org.apache.spark.sql.SaveMode

/**
 */
object MergeKeyWord {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionBase.createSparkSession()
    import spark.implicits._
    spark.sql("use program")
    val mergeDF = spark.sql(
      """
        | select w.item_id, collect_set(w.word) as keyword1, collect_set(k.word) as keyword2
        | from keyword_tr w join keyword_tfidf k on (w.item_id = k.item_id)
        | group by w.item_id
        |""".stripMargin)
    mergeDF.rdd.map(row => {
      val itemID = row.getAs[Int]("item_id")
      val keyword1 = row.getAs[Seq[String]]("keyword1")
      val keyword2 = row.getAs[Seq[String]]("keyword2")
      val keywords = keyword1.union(keyword2).distinct.toArray
      (itemID, keywords)
    }).toDF("item_id", "keyword")
      .write
      .mode(SaveMode.Overwrite)
//      .saveAsTable("item_keyword")
      .insertInto("item_keyword")
  }
}
