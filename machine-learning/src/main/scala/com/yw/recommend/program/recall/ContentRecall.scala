package com.yw.recommend.program.recall

import com.yw.recommend.program.util.{HBaseUtil, PropertiesUtils, SparkSessionBase}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
 * 内容召回
 */
object ContentRecall {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionBase.createSparkSession()
    val df = spark.sql(
      """
        | select a.sn, a.item_id, a.duration, b.length
        | from program.user_action a
        | join program.item_info b on a.item_id = b.item_id where a.sn != 'unknown'
        |""".stripMargin)

    // flatMap: 1对N（N可以为0） 如果使用map，还得再加filter过滤 scalaDuration > 0.1
    val itemID2userID = df.rdd.flatMap(row => {
      val list = new ListBuffer[(Int, String)]()
      val userID = row.getAs[String]("sn")
      println(userID)
      val itemID = row.getAs[Int]("item_id")
      val duration = row.getAs[Long]("duration")
      val length = row.getAs[Long]("length")

      if (duration < length) {
        val scalaDuration = (duration * 1.0) / length
        if (scalaDuration > 0.1) {
          list.+=((itemID, userID))
        }
      }
      list.iterator

      // 用户可能会点击这个节目N多次，那么在计算内容召回的时候，应该去重，不然内容召回表中会有大量重复数据
    }).distinct()

    val table = PropertiesUtils.getProp("similar.hbase.table")
    val conf = HBaseUtil.getConf(table)

    val hbaseRdd: RDD[(ImmutableBytesWritable, Result)] = spark.sparkContext.newAPIHadoopRDD(
      conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    val similarPro = hbaseRdd.flatMap(data => {
      val list = new ListBuffer[(Int, Int)]()
      val result = data._2
      for (rowKv <- result.rawCells()) {
        val rowkey = new String(rowKv.getRowArray, rowKv.getRowOffset, rowKv.getRowLength, "UTF-8")
        val colName = new String(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength, "UTF-8")
//        val value = new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8")
        list.+=((rowkey.toInt, colName.toInt))
      }
      list.iterator
    })

    itemID2userID.join(similarPro).map(x => {
      (x._2._1, x._2._2)
    }).groupByKey().foreachPartition(partition => {
//      user.recall.hbase.table=recall
//      user.history.recall.hbase.table=history_recall
      val recallTableName = PropertiesUtils.getProp("user.recall.hbase.table")
      val hisRecallTableName = PropertiesUtils.getProp("user.history.recall.hbase.table")
      val conf = HBaseUtil.getHBaseConfiguration
//            conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
      val conn = ConnectionFactory.createConnection(conf)
      val recallTable = HBaseUtil.getTable(conf, recallTableName)
      val hisRecallTable = HBaseUtil.getTable(conf, hisRecallTableName)
      for (elem <- partition) {
        val userID = elem._1
        val hisRecalls = HBaseUtil.getRecord(hisRecallTableName, userID, conn).map(_.toInt).toSet
        val itemIDs = elem._2.toSet
        val diff = itemIDs -- hisRecalls

        if (diff.nonEmpty) {
          val recall = diff.mkString("|")
          // 添加找到recall
          val put = new Put(Bytes.toBytes(userID))
          put.addColumn(Bytes.toBytes("content"), Bytes.toBytes("item"), Bytes.toBytes(recall))
          recallTable.put(put)
          // 添加到历史recall表
          val hput = new Put(Bytes.toBytes(userID))
          hput.addColumn(Bytes.toBytes("recommend"), Bytes.toBytes("recommend"), Bytes.toBytes(recall))
          hisRecallTable.put(hput)
        }
      }
      conn.close()
      recallTable.close()
      hisRecallTable.close()
    })
    df.show()
  }
}
