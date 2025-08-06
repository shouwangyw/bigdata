package com.yw.recommend.program.feature

import com.yw.recommend.program.util.{PropertiesUtils, RedisUtil, SparkSessionBase}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
 * 特征索引
 */
object UpdateFeatureIndex {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionBase.createSparkSession()
    val table = PropertiesUtils.getProp("user.profile.hbase.table")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", PropertiesUtils.getProp("hbase.zookeeper.property.clientPort"))
    conf.set("hbase.zookeeper.quorum", PropertiesUtils.getProp("hbase.zookeeper.quorum"))
    conf.set("zookeeper.znode.parent", PropertiesUtils.getProp("zookeeper.znode.parent"))
    conf.set(TableInputFormat.INPUT_TABLE, table)

    val hbaseRdd: RDD[(ImmutableBytesWritable, Result)] = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val distinctWords = hbaseRdd.flatMap(data => {
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
      // zipWithUniqueID  不连续 唯一
      .zipWithIndex() //连续 唯一
      .collectAsMap()

    RedisUtil.init("node01", 6379)
    distinctWords.map(data => {
      RedisUtil.insertValue(5, "kw", data._1, data._2.toString)
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

    provinceMap.map(data => {
      RedisUtil.insertValue(5, "province", data._1, data._2.toString)
    })

    cityMap.map(data => {
      RedisUtil.insertValue(5, "city", data._1, data._2.toString)
    })

    spark.stop()
  }
}
