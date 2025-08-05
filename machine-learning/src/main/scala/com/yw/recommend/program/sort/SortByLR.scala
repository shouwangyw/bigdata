//package com.yw.recommend.program.sort
//
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.Result
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
//import org.apache.spark.ml.feature.VectorAssembler
//import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
//import org.apache.spark.rdd.RDD
//import com.yw.recommend.program.util.{PropertiesUtils, SparkSessionBase}
//import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
//
//import scala.collection.mutable.ListBuffer
//
//
///**
//  * 离线排序模型  CTR预估
//  *
//  * 给定一条用户行为数据，预估该节目被观看的概率
//  *
//  * 预估需要训练一个逻辑回归算法模型
//  * （1）构建训练集
//  * （2）依据模型来预估
//  */
//object SortByLR {
//  def main(args: Array[String]): Unit = {
//
//    val lr = new LogisticRegression()
//    val model = lr.setFeaturesCol("features").setLabelCol("label").fit(trainDF)
//    model.save("/recommend/program/models/lrModel.model")
//
//    val online_model = LogisticRegressionModel.load("/recommend/program/models/lrModel.model")
//
//    val res_transfrom = online_model
//      .transform(trainDF)
//      .select("label","probability")
//
//    /**
//      * +----------------------------------------+-------------------------------------------+----------+
//      * |rawPrediction                           |probability                                |prediction|
//      * +----------------------------------------+-------------------------------------------+----------+
//      * |[-19.648230024058805,19.648230024058805]|[2.930097825333636E-9,0.9999999970699021]  |1.0       |
//      * |[-22.387409870586566,22.387409870586566]|[1.8935266811269158E-10,0.9999999998106472]|1.0       |
//      * |[-18.52044229967499,18.52044229967499]  |[9.050531888736478E-9,0.9999999909494682]  |1.0       |
//      * |[-19.529825296476012,19.529825296476012]|[3.2984100098723075E-9,0.99999999670159]   |1.0       |
//      * |[-19.198902655070647,19.198902655070647]|[4.592218209063178E-9,0.9999999954077818]  |1.0       |
//      * |[-20.632793911879954,20.632793911879954]|[1.0946907726411503E-9,0.9999999989053092] |1.0       |
//      * |[-19.590826949857394,19.590826949857394]|[3.1032156614001757E-9,0.9999999968967843] |1.0       |
//      * |[-19.385273038815797,19.385273038815797]|[3.811385587626314E-9,0.9999999961886143]  |1.0       |
//      * 置信度
//      */
//    res_transfrom.show(false)
//    res_transfrom.printSchema()
//
//
//    val scoreLabelRDD = res_transfrom.rdd.map(row=>{
//      val label = row.getAs[Int]("label").toDouble
//      val probability = row.getAs[DenseVector]("probability")(1)
//      (label,probability)
//    })
//
//    val metrics = new BinaryClassificationMetrics(scoreLabelRDD)
//    val auc = metrics.areaUnderROC()
//    println("auc:" + auc )
//  }
//}
