package com.yw.recommend.program.feature

import com.yw.recommend.program.util.{PropertiesUtils, SparkSessionBase}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.rdd.RDD

import com.yw.recommend.program.util.{PropertiesUtils, SparkSessionBase}
import scala.collection.mutable.ListBuffer

object FeaturesFactory {

  //构建特征工程
  def getLRFeatures = {
    /**
      * 构建训练集-特征工程
      */
    val session = SparkSessionBase.createSparkSession()
    val table = PropertiesUtils.getProp("user.profile.hbase.table")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", PropertiesUtils.getProp("hbase.zookeeper.property.clientPort"))
    conf.set("hbase.zookeeper.quorum", PropertiesUtils.getProp("hbase.zookeeper.quorum"))
    conf.set("zookeeper.znode.parent", PropertiesUtils.getProp("zookeeper.znode.parent"))
    conf.set(TableInputFormat.INPUT_TABLE, table)

    var hbaseRdd: RDD[(ImmutableBytesWritable, Result)] = session.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    hbaseRdd = hbaseRdd.cache()
    import session.implicits._


    //读取hbase信息,扫描信息
    var distinctWords = hbaseRdd.flatMap(data => {
      val list = new ListBuffer[String]
      val result = data._2
      for (rowKv <- result.rawCells()) {
        val rowkey = new String(rowKv.getRowArray, rowKv.getRowOffset, rowKv.getRowLength, "UTF-8")
        val colName = new String(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength, "UTF-8")
        val value = new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8")
        if (value.contains("keyWord")) {
          val elems = value.split("\t")
          val words = elems.map(x => {
            if (x.contains("keyWord")) {
              x.split(":")(1)
            } else if (x.contains("score")) {
              x.split("\\|")(0)
            } else {
              x
            }
          })
          list.++=(words.toSeq)
        }
      }
      list.iterator
    }).distinct()
      .zipWithIndex()
      .collectAsMap()

    val distinctWordsBroad = session.sparkContext.broadcast(distinctWords)

    val labelFeatures = hbaseRdd.flatMap(data => {
      val result = data._2
      val dict = distinctWordsBroad.value
      val list = new ListBuffer[((String, Int), DenseVector)]
      for (rowKv <- result.rawCells()) {
        val userID = new String(rowKv.getRowArray, rowKv.getRowOffset, rowKv.getRowLength, "UTF-8")
        val colName = new String(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength, "UTF-8")
        val value = new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8")

        if (colName.contains("itemID")) {
          val itemID = colName.split(":")(1).toInt
          val elems = value.split("\t")
          val score = value.split("\\|")(1).split(":")(1).toDouble
          val words = elems.map(x => {
            if (x.contains("keyWord")) {
              x.split(":")(1)
            } else if (x.contains("score")) {
              x.split("\\|")(0)
            } else {
              x
            }
          })

          val indexs = words.map(dict.get(_).get.toInt).sorted

          val vector = new SparseVector(dict.size, indexs, Array.fill(indexs.length)(score))
          list.+=(((userID, itemID), vector.toDense))
        }
      }
      list.iterator
    })


    val provinceWithCity = hbaseRdd.map(data => {
      val result = data._2
      var userID = ""
      var province = ""
      var city = ""
      for (rowKv <- result.rawCells()) {
        userID = new String(rowKv.getRowArray, rowKv.getRowOffset, rowKv.getRowLength, "UTF-8")
        val colName = new String(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength, "UTF-8")
        val value = new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8")
        if ("province".equals(colName)) {
          province = value
        }
        if ("city".equals(colName)) {
          city = value
        }
      }
      (userID, (province, city))
    })

    val provinceMap = provinceWithCity.map(_._2._1).distinct().zipWithIndex().collectAsMap()
    val cityMap = provinceWithCity.map(_._2._2).distinct().zipWithIndex().collectAsMap()
    val provinceMapBroad = session.sparkContext.broadcast(provinceMap)
    val cityMapBroad = session.sparkContext.broadcast(cityMap)

    val provinceWithCityFeatures = provinceWithCity.map(data => {
      val userID = data._1
      val province = data._2._1
      val provinceMap = provinceMapBroad.value
      val cityMap = cityMapBroad.value
      val provinceIndex = Array(provinceMap.get(province).get.toInt)
      val provinceFeatures = new SparseVector(provinceMap.size, provinceIndex, Array.fill(provinceIndex.length)(1.0))
      val city = data._2._2
      println(city)
      val cityIndex = Array(cityMap.get(city).get.toInt)
      val cityFeatures = new SparseVector(cityMap.size, cityIndex, Array.fill(cityIndex.length)(1.0))
      (userID, (provinceFeatures.toDense, cityFeatures.toDense))
    })

    /**
      * 用户特征已经准备完毕
      *
      * 获取用户行为数据，关联特征
      */
    val itemFeatureDF = session.sql("" +
      "select a.sn,a.item_id,a.duration,b.features " +
      "from program.user_action a join " +
      "tmp_program.tmp_keyword_weight b " +
      "on (a.item_id = b.item_id) ")
    itemFeatureDF.show()

    /**
      * root
      * |-- sn: string (nullable = true)
      * |-- item_id: integer (nullable = true)
      * |-- duration: long (nullable = true)
      * |-- features: vector (nullable = true)
      */
    val userID2ActionRDD = itemFeatureDF.rdd.map(row => {
      val sn = row.getAs[String]("sn")
      val itemID = row.getAs[Int]("item_id")
      val duration = row.getAs[Long]("duration")
      val features = row.getAs[DenseVector]("features")
      (sn, (itemID, duration, features))
    })


    val itemInfo = session.table("program.item_info")
    val itemID2LengthMap = itemInfo.map(row => {
      val itemID = row.getAs[Int]("item_id")
      val length = row.getAs[Long]("length")
      (itemID, length)
    }).collect().toMap

    //由于节目信息数据量并不是很大，完全可以放入在广播变量中保存
    val itemID2LengthMapBroad = session.sparkContext.broadcast(itemID2LengthMap)

    val featuresDF = userID2ActionRDD.join(provinceWithCityFeatures).map(row => {
      val userID = row._1
      val (itemID, duration, features) = row._2._1
      val (provinceVector, cityVector) = row._2._2
      ((userID, itemID), (duration, features, provinceVector, cityVector))
    }).join(labelFeatures)
      .map(row => {
        val (userID, itemID) = row._1
        val (duration, features, provinceVector, cityVector) = row._2._1
        val userLabelVector = row._2._2

        val itemID2LengthMap = itemID2LengthMapBroad.value
        val length = itemID2LengthMap.get(itemID).get
        // TODO: 数据需要修改
        val label = if (duration < length) {
          val durationScale = (duration * 1.0) / length
          if (durationScale > 0.1) 1 else 0
        } else 1
        (userID, itemID, duration, features, provinceVector, cityVector, userLabelVector, label)
      }).toDF("userID", "itemID", "duration", "program_features", "province_Vector", "city_Vector", "userLabel_Vector", "label")


    val assem = new VectorAssembler()
    val trainDF =
      assem
        .setInputCols(Array("program_features", "province_Vector", "city_Vector", "userLabel_Vector"))
        .setOutputCol("features")
        .transform(featuresDF)
    trainDF
  }



}
