package com.yw.spark.example.cases

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author yangwei
  */
object Case09_SparkWithHBase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 1. 创建HBase的环境参数
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181");
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "spark_hbase")

//    // 2. 设置过滤器，还可以设置起始和结束rowkey
//    val scan = new Scan
//    scan.setFilter(new RandomRowFilter(0.5f))
//    // 设置scan对象，让filter生效(序列化)
//    hbaseConf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

    // 3. 读取HBase数据，生成RDD
    val resultRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])

    resultRDD.foreach(x => {
      // 查询出来的结果集存在 (ImmutableBytesWritable, Result)第二个元素
      val result = x._2
      // 获取行键
      val rowKey = Bytes.toString(result.getRow)
      val name = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
      val age = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")))
      println(rowKey + ":" + name + ":" + age)
    })

    // 4. 向HBase表写入数据
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "spark_hbase_out")
    val job = Job.getInstance(hbaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    // 5. 封装输出结果 resultRDD: RDD[(ImmutableBytesWritable, Result)]
    val outRDD: RDD[(ImmutableBytesWritable, Put)] = resultRDD.mapPartitions(eachPartition => {
      eachPartition.map(keyAndEachResult => {
        val result = keyAndEachResult._2
        val rowKey = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
        val age = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")))

        val put = new Put(Bytes.toBytes(rowKey))
        val immutableBytesWritable = new ImmutableBytesWritable(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(name))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(age))
        // 向 HBase 插入数据，需要 rowKey 和 put 对象
        (immutableBytesWritable, put)
      })
    })

    // 6. 调用API Output the RDD to any Hadoop-supported storage system with new Hadoop API
    outRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
    sc.stop()
  }
}
