package com.yw.recommend.program

import com.yw.recommend.program.util.{SegmentWordUtil, SparkSessionBase}
import org.apache.spark.ml.feature.Word2Vec

/**
 */
object ComputeWTV {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    // 通过SparkSessionBase创建Spark会话
    val spark = SparkSessionBase.createSparkSession()
    import spark.implicits._
    spark.sql("use program")
    // 获取节目信息，然后对其进行分词
//    val articleDF = session.sql("select * from item_info limit 20")
    val articleDF = spark.table("item_info")
    val seg = new SegmentWordUtil()
    val words_df = articleDF.rdd.mapPartitions(seg.segeFun).toDF("item_id", "words")

    /**
     * vectorSize: 词向量长度
     * minCount：过滤词频小于5的词
     * windowSize：window窗口大小
     */
    val w2v = new Word2Vec
    w2v.setInputCol("words")
    w2v.setOutputCol("model")
    w2v.setVectorSize(128)
    w2v.setMinCount(3)
    w2v.setWindowSize(5)

    val w2vModel = w2v.fit(words_df)
    w2vModel.write.overwrite().save("/recommend/program/models/w2v.model")
    spark.read.parquet("/recommend/program/models/w2v.model/data/*").show(false)
    spark.close()
  }
}
