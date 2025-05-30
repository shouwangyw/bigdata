package com.yw.flink.example.scalacases.case14_flinksql

import com.yw.flink.example.StationLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
  * Flink DataStream转换成Table - tableEnvironment.createTemporaryView(str,dataStream)
  */
object Case07_CreateTemporaryView {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val ds: DataStream[StationLog] = env.socketTextStream("node5", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        StationLog(arr(0), arr(1), arr(2), arr(3), arr(4).toLong, arr(5).toLong)
      })

    tableEnv.createTemporaryView("xx", ds);

    val table: Table = tableEnv.from("xx")
    table.printSchema()
    table.execute().print()
  }
}
