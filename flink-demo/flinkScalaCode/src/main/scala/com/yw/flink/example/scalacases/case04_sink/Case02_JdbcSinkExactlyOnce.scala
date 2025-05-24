package com.yw.flink.example.scalacases.case04_sink

import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource
import com.yw.flink.example.StationLog
import org.apache.flink.connector.jdbc.{JdbcExactlyOnceOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.function.SerializableSupplier

import java.sql.PreparedStatement
import javax.sql.XADataSource

/**
  * Flink JDBC Exactly-once Sink
  */
object Case02_JdbcSinkExactlyOnce {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000)
    import org.apache.flink.streaming.api.scala._
    //003,186,188,busy,3000,30
    val ds1: DataStream[String] = env.socketTextStream("node5", 9999)
    val ds2: DataStream[StationLog] = ds1.map(one => {
      val split: Array[String] = one.split(",")
      val sid: String = split(0)
      val callOut: String = split(1)
      val callIn: String = split(2)
      val callType: String = split(3)
      val callTime: Long = split(4).toLong
      val duration: Long = split(5).toLong
      StationLog(sid, callOut, callIn, callType, callTime, duration)
    })

    //准备JDBC 写出的对象
    /**
      * JdbcSink.sink(
      * sqlDmlStatement,      // 必须指定，SQL语句
      * jdbcStatementBuilder, // 必须指定，给SQL语句设置参数
      * jdbcExecutionOptions, // 可选，指定写出参数，如：提交周期、提交批次大小、重试时间，建议指定。
      * jdbcConnectionOptions // 必须指定，数据库连接参数
      * );
      */

    val jdbcSink: SinkFunction[StationLog] = JdbcSink.exactlyOnceSink[StationLog](
      "insert into station_log values (?,?,?,?,?,?)",
      new JdbcStatementBuilder[StationLog] {
        override def accept(pst: PreparedStatement, station: StationLog): Unit = {
          pst.setString(1, station.sid)
          pst.setString(2, station.callOut)
          pst.setString(3, station.callIn)
          pst.setString(4, station.callType)
          pst.setLong(5, station.callTime)
          pst.setLong(6, station.duration)
        }
      },
      JdbcExecutionOptions.builder()
        .withBatchSize(500)
        .withBatchIntervalMs(200)
        .withMaxRetries(0).build(),
      JdbcExactlyOnceOptions.builder().withTransactionPerConnection(true).build(),

      new SerializableSupplier[XADataSource]() {
        override def get: XADataSource = {
          val xaDataSource: MysqlXADataSource = new MysqlXADataSource
          xaDataSource.setURL("jdbc:mysql://node2:3306/mydb?useSSL=false")
          xaDataSource.setUser("root")
          xaDataSource.setPassword("123456")
          return xaDataSource
        }
      }
    )

    //将数据写出到JDBC中
    ds2.addSink(jdbcSink)
    env.execute()
  }
}
