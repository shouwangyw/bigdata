package com.yw.recommend.program.program

import com.yw.recommend.program.util.{SegmentWordUtil, SparkSessionBase}
import org.apache.spark.ml.feature.{CountVectorizer, IDF}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.SaveMode

import scala.collection.mutable.ListBuffer

/**
 * 对节目信息分词，然后求TF-IDF值
 * hive --service metastore 2>&1 >> /opt/meta.log &
 */
object ComputeTFIDF {
  def main(args: Array[String]): Unit = {
    // 通过SparkSessionBase创建Spark会话
    val session = SparkSessionBase.createSparkSession()
    import session.implicits._
    /**
     * 查询指定使用hive哪一个数据库，有两种方式：
     *   1. sql("use database")
     *   2. sql(select * from program.item_info)
     */
    session.sql("use program")
    // 获取节目信息，然后对其进行分词
    val articleDF = session.sql("select * from item_info limit 20")
//    val articleDF = session.table("item_info")

    // 分词
    val seg = new SegmentWordUtil()
    val words_df = articleDF.rdd.mapPartitions(seg.segeFun).toDF("item_id", "words")
    words_df.show(false)

    // 创建CountVectorizer对象，统计所有影响的词，形成词袋
    val countVectorizer = new CountVectorizer()
    countVectorizer.setInputCol("words")
    countVectorizer.setOutputCol("features")
    countVectorizer.setVocabSize(10000)
    // 词必须出现在至少一篇文章中，如果是一个0-1的数字，则代表概率
    countVectorizer.setMinDF(1.0)

    // 训练词袋模型
    val cvModel = countVectorizer.fit(words_df)
//    // 保存词袋模型到hdfs上
//        cvModel.write.overwrite().save("hdfs://node1:9000/recommend/program/models/CV.model")
//
//    // 通过spark sql读取模型内容
//    session.read.parquet("hdfs://node1:9000/recommend/program/models/CV.model/data/*").show()
//    // 这是所有的词
//        cvModel.vocabulary.foreach(println)

    val cv_result = cvModel.transform(words_df)
    cv_result.show(10, false)


    // 创建IDF对象
    val idf = new IDF()
    idf.setInputCol("features")
    idf.setOutputCol("features_tfidf")
    // 计算每个词的逆文档频率
    val idfModel = idf.fit(cv_result)
    idfModel.write.overwrite().save("hdfs://node1:9000/recommend/program/models/IDF.model")

    /**
     * tf：w1：10   w1：100
     *
     * idf基于整个语料库计算出来的
     * word ： idf值
     */
    session.read.parquet("hdfs://node01:9000/recommend/program/models/IDF.model/data").show(10, false)

    /**
     * 将每个单词对应的IDF（逆文档频率） 保存在Hive表中
     */
    //整理数据格式（index,word,IDF）
    val keywordsWithIDFList = new ListBuffer[(Int, String, Double)]
    val words = cvModel.vocabulary
    val idfs = idfModel.idf.toArray
    for (index <- words.indices) {
      keywordsWithIDFList += ((index, words(index), idfs(index)))
    }
    //保存数据
    session.sql("use tmp_program")
    session
      .sparkContext
      .parallelize(keywordsWithIDFList)
      .toDF("index", "keywords", "idf")
      .write
      .mode(SaveMode.Overwrite)
      .insertInto("keyword_idf")


    //CVModel->CVResult->IDFModel->CVResult->TFIDFResult

    val tfIdfResult = idfModel.transform(cv_result)
    tfIdfResult.show()

    //根据TFIDF来排序
    val keyword2TFIDF = tfIdfResult.rdd.mapPartitions(partition => {
      val rest = new ListBuffer[(Long, Int, Double)]
      val topN = 20

      while (partition.hasNext) {
        val row = partition.next()
        var idfVals: List[Double] = row.getAs[SparseVector]("features_tfidf").values.toList
        val tmpList = new ListBuffer[(Int, Double)]

        for (i <- idfVals.indices)
          tmpList += ((i, idfVals(i)))


        val buffer = tmpList.sortBy(_._2).reverse
        for (item <- buffer.take(topN))
          rest += ((row.getAs[Long]("item_id"), item._1, item._2))
      }
      rest.iterator
    }).toDF("item_id", "index", "tfidf")
    keyword2TFIDF.show(10)


    keyword2TFIDF.createGlobalTempView("keywordsByTable")
    //获取索引对应的单词，组织格式 保存Hive表
    session.sql("select * from keyword_idf a join global_temp.keywordsByTable b on a.index = b.index")
      .select("item_id", "word", "tfidf")
      .write
      .mode(SaveMode.Overwrite)
      .insertInto("keyword_tfidf")
    session.close()
  }
}

