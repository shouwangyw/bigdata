package com.yw.flink.example.scalacases.case04_sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  * Flink 自定义Sink 输出
  *
  */
object Case05_CustomSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._
    val ds: DataStream[String] = env.socketTextStream("node5", 9999)
    ds.addSink(new RichSinkFunction[String] {

      var conf: org.apache.hadoop.conf.Configuration = _
      var conn: Connection = _

      //初始化资源，数据处理前只执行一次
      override def open(parameters: Configuration): Unit = {
        conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "node3,node4,node5")
        conf.set("hbase.zookeeper.prperty.clientPort", "2181")
        conn = ConnectionFactory.createConnection(conf)
      }

      //数据来一条调用一次
      override def invoke(value: String, context: SinkFunction.Context): Unit = {
        val split: Array[String] = value.split(",")
        //001,186,187,success,1000,10
        val sid: String = split(0)
        val callOut: String = split(1)
        val callIn: String = split(2)
        val rowkey = sid + callOut + callIn
        val callType: String = split(3)
        val callTime: String = split(4)
        val duration: String = split(5)

        //准备表对象
        val table: Table = conn.getTable(TableName.valueOf("flink-sink-hbase"))
        //准备put对象
        val put = new Put(rowkey.getBytes())
        //准备列族和列
        put.addColumn("cf".getBytes(), "callOut".getBytes(), callOut.getBytes())
        put.addColumn("cf".getBytes(), "callIn".getBytes(), callIn.getBytes())
        put.addColumn("cf".getBytes(), "callType".getBytes(), callType.getBytes())
        put.addColumn("cf".getBytes(), "callTime".getBytes(), callTime.getBytes())
        put.addColumn("cf".getBytes(), "duration".getBytes(), duration.getBytes())

        //插入数据
        table.put(put)

        //关闭对象
        table.close()

      }

      //关闭资源执行，Flink程序完成后执行一次
      override def close(): Unit = conn.close()
    })

    env.execute()

  }

}
