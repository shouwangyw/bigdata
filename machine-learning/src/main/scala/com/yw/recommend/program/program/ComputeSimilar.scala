package com.yw.recommend.program.program

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.ml.feature.{BucketedRandomProjectionLSH, Word2VecModel}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.Row
import com.yw.recommend.program.util.{HBaseUtil, PropertiesUtils, SparkSessionBase}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * ntpdate ntp1.aliyun.com
  */
object ComputeSimilar {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val spark = SparkSessionBase.createSparkSession()
    spark.sql("use program")
    import spark.implicits._

    //    val keyWord2WeightDF = session.table("keyword_tr").limit(1000)
    val keyWord2WeightDF = spark.table("keyword_tfidf")
    val word2Weight = keyWord2WeightDF.rdd.map(row => {
      val itemID = row.getAs[Int]("item_id")
      val word = row.getAs[String]("word")
      val tr = row.getAs[Double]("tfidf")
      (itemID + "_" + word, tr)
    }).collect().toMap
    val word2WeightBroad = spark.sparkContext.broadcast(word2Weight)


    val word2VecModel = Word2VecModel.load("hdfs://node01:9000/recommend/program/models/w2v.model")
    val word2VecMap = word2VecModel.getVectors.collect().map(row => {
      val vector = breeze.linalg.DenseVector(row.getAs[DenseVector]("vector").toArray)
      val word = row.getAs[String]("word")
      (word, vector)
    }).toMap
    val word2VecMapBroad = spark.sparkContext.broadcast(word2VecMap)

    val word2Index = spark.table("keyword_idf").rdd.map(row => {
      val index = row.getAs[Int]("index")
      val word = row.getAs[String]("word")
      (word, index)
    }).collectAsMap()

    val word2IndexBroad = spark.sparkContext.broadcast(word2Index)

    //    val keyWordDF = session.table("item_keyword")
    val keyWordDF = spark.sql("select * from item_keyword limit 1000")
    val featuresDF = keyWordDF.map(row => {
      val map = mutable.HashMap[String, Double]()
      val word2VecMap = word2VecMapBroad.value
      val word2Weight = word2WeightBroad.value
      val word2Index = word2IndexBroad.value
      val itemID = row.getAs[Int]("item_id")
      val keywords = row.getAs[Seq[String]]("keyword")
      var index = 0
      val indexs = new ArrayBuffer[Int]()
      val values = new ArrayBuffer[Double]()
      for (word <- keywords) {
        val weight = word2Weight.getOrElse(itemID + "_" + word, 1.0)
        var nWht = 0d
        if (word2VecMap.contains(word)) {
          val trScore = word2VecMap(word)
          val newVector = trScore * weight
          nWht = newVector.toArray.sum / newVector.length
        } else {
          nWht = weight
        }
        if (word2Index.contains(word)) {
          indexs += (word2Index.get(word).get)
          values += (nWht)
        }
      }
      val vector = new SparseVector(word2Index.size, indexs.toArray.sorted, values.toArray)
      (itemID, vector.toDense)
    }
    ).toDF("item_id", "features")
    //    featuresDF.write.mode(SaveMode.Overwrite).saveAsTable("tmp_keyword_weight")

    val rddArr = featuresDF.randomSplit(Array(0.7, 0.3))
    val train = rddArr(0)
    val test = rddArr(1)

    val brpls = new BucketedRandomProjectionLSH()
    brpls.setInputCol("features")
    brpls.setOutputCol("hashes")
    //桶个数
    brpls.setBucketLength(10.0)
    val model = brpls.fit(train)

    val similar = model.approxSimilarityJoin(featuresDF, featuresDF, 2.0, "EuclideanDistance")
    /**
      * 分到同一个桶中
      * +--------------------+--------------------+-----------------+
      * |            datasetA|            datasetB|EuclideanDistance|
      * +--------------------+--------------------+-----------------+
      * |[337747, [0.0,8.2...|[433272, [0.0,8.2...|              0.0|
      * |[400803, [0.0,8.2...|[364358, [0.0,8.2...|              0.0|
      * |[407580, [0.0,8.2...|[256381, [0.0,8.2...|              0.0|
      * |[43163, [0.0,8.24...|[538311, [0.0,8.2...|              0.0|
      * |[43163, [0.0,8.24...|[201201, [0.0,8.2...|              0.0|
      * |[563779, [0.0,8.2...|[114265, [0.0,8.2...|              0.0|
      * |[524706, [0.0,8.2...|[206419, [0.0,8.2...|              0.0|
      * |[330830, [0.0,8.2...|[520159, [0.0,8.2...|              0.0|
      * |[418635, [0.0,8.2...|[508540, [0.0,8.2...|              0.0|
      * |[368514, [0.0,8.2...|[393038, [0.0,8.2...|              0.0|
      * +--------------------+--------------------+-----------------+
      */

    val tableName = PropertiesUtils.getProp("similar.hbase.table")
    similar.toDF()
      .rdd
      .foreachPartition(partition => {
        val conf = HBaseUtil.getHBaseConfiguration()
//        conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
        val htable = HBaseUtil.getTable(conf,tableName)
        for (row <- partition) {
          if (row.getAs[Double]("EuclideanDistance") < 1) {
            val aItemID = row.getAs[Row]("datasetA").getAs[Int](0)
            val bItemID = row.getAs[Row]("datasetB").getAs[Int](0)
            val dist = row.getAs[Double]("EuclideanDistance")
            if (aItemID != bItemID) {
              val put = new Put(Bytes.toBytes(aItemID + ""))
              put.addColumn(Bytes.toBytes("similar"), Bytes.toBytes(bItemID + ""), Bytes.toBytes(dist + ""))
              htable.put(put)
            }
          }
        }
      })
    spark.close()
  }
}
