package com.yw.spark.example.core.cases

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object Case10_SparkOnHBase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "spark_hbase")

    val hbaseContext = new HBaseContext(sc, hbaseConf)
    val scan = new Scan()

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = hbaseContext.hbaseRDD(TableName.valueOf("spark_hbase"), scan)

    hbaseRDD.map(eachResult => {

      val result: Result = eachResult._2
      val rowKey = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
      val age = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")))
      println(rowKey + ":" + name + ":" + age)
    }).foreach(println)
    sc.stop()
  }
}
